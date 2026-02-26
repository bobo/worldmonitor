declare const process: { env: Record<string, string | undefined> };

/**
 * Environment-based key prefix to avoid collisions when multiple deployments
 * share the same Upstash Redis instance (M-6 fix).
 */
function getKeyPrefix(): string {
  const env = process.env.VERCEL_ENV; // 'production' | 'preview' | 'development'
  if (!env || env === 'production') return '';
  const sha = process.env.VERCEL_GIT_COMMIT_SHA?.slice(0, 8) || 'dev';
  return `${env}:${sha}:`;
}

let cachedPrefix: string | undefined;
function prefixKey(key: string): string {
  if (cachedPrefix === undefined) cachedPrefix = getKeyPrefix();
  if (!cachedPrefix) return key;
  return `${cachedPrefix}${key}`;
}

export async function getCachedJson(key: string): Promise<unknown | null> {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  try {
    const resp = await fetch(`${url}/get/${encodeURIComponent(prefixKey(key))}`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(3_000),
    });
    if (!resp.ok) return null;
    const data = (await resp.json()) as { result?: string };
    return data.result ? JSON.parse(data.result) : null;
  } catch {
    return null;
  }
}

export async function setCachedJson(key: string, value: unknown, ttlSeconds: number): Promise<void> {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return;
  try {
    // Atomic SET with EX — single call avoids race between SET and EXPIRE (C-3 fix)
    await fetch(`${url}/set/${encodeURIComponent(prefixKey(key))}/${encodeURIComponent(JSON.stringify(value))}/EX/${ttlSeconds}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(3_000),
    });
  } catch { /* best-effort */ }
}

/**
 * Batch GET using Upstash pipeline API — single HTTP round-trip for N keys.
 * Returns a Map of key → parsed JSON value (missing/failed keys omitted).
 */
export async function getCachedJsonBatch(keys: string[]): Promise<Map<string, unknown>> {
  const result = new Map<string, unknown>();
  if (keys.length === 0) return result;

  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return result;

  try {
    const pipeline = keys.map((k) => ['GET', prefixKey(k)]);
    const resp = await fetch(`${url}/pipeline`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(pipeline),
      signal: AbortSignal.timeout(3_000),
    });
    if (!resp.ok) return result;

    const data = (await resp.json()) as Array<{ result?: string }>;
    for (let i = 0; i < keys.length; i++) {
      const raw = data[i]?.result;
      if (raw) {
        try { result.set(keys[i]!, JSON.parse(raw)); } catch { /* skip malformed */ }
      }
    }
  } catch { /* best-effort */ }
  return result;
}

/**
 * In-flight request coalescing map.
 * When multiple concurrent requests hit the same cache key during a miss,
 * only the first triggers the upstream fetch — others await the same promise.
 * This eliminates duplicate upstream API calls within a single Edge Function invocation.
 */
const inflight = new Map<string, Promise<unknown>>();

// ---------------------------------------------------------------------------
// Distributed lock — prevents cross-instance cache stampede
// ---------------------------------------------------------------------------

const LOCK_TTL = 15; // seconds — must exceed worst-case upstream fetch duration
const LOCK_POLL_INTERVAL_MS = 250;
const LOCK_POLL_MAX_ATTEMPTS = 6; // 6 × 250ms = 1.5s max wait

async function acquireLock(lockKey: string): Promise<boolean> {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return false;
  try {
    const resp = await fetch(
      `${url}/set/${encodeURIComponent(prefixKey(lockKey))}/1/NX/EX/${LOCK_TTL}`,
      {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}` },
        signal: AbortSignal.timeout(3_000),
      },
    );
    if (!resp.ok) return false;
    const data = (await resp.json()) as { result: 'OK' | null };
    return data.result === 'OK';
  } catch {
    return false;
  }
}

async function releaseLock(lockKey: string): Promise<void> {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return;
  try {
    await fetch(`${url}/del/${encodeURIComponent(prefixKey(lockKey))}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(3_000),
    });
  } catch { /* best-effort */ }
}

/**
 * On cache miss, try to acquire a distributed lock so only one instance
 * fetches upstream. Losers poll Redis for the winner's result.
 * Falls open (proceeds with fetch) when Redis is unavailable.
 */
async function fetchWithLock<T>(
  key: string,
  ttlSeconds: number,
  fetcher: () => Promise<T>,
): Promise<T | null> {
  const lockKey = `lock:${key}`;
  const acquired = await acquireLock(lockKey);

  if (!acquired) {
    // Another instance is fetching — poll for its result
    for (let i = 0; i < LOCK_POLL_MAX_ATTEMPTS; i++) {
      await new Promise<void>((r) => setTimeout(r, LOCK_POLL_INTERVAL_MS));
      const filled = await getCachedJson(key);
      if (filled !== null) return filled as T;
    }
    // No Redis or winner didn't finish — fall open
    return fetcher();
  }

  // We are the leader — fetch, cache, release
  try {
    const result = await fetcher();
    if (result != null) {
      await setCachedJson(key, result, ttlSeconds);
    }
    return result;
  } finally {
    await releaseLock(lockKey);
  }
}

/**
 * Check cache, then fetch with coalescing on miss.
 * Uses both in-process coalescing (inflight map) and cross-instance
 * distributed locking (Redis SET NX) to prevent stampedes.
 */
export async function cachedFetchJson<T>(
  key: string,
  ttlSeconds: number,
  fetcher: () => Promise<T>,
): Promise<T | null> {
  const cached = await getCachedJson(key);
  if (cached !== null) return cached as T;

  // In-process coalescing (same instance)
  const existing = inflight.get(key);
  if (existing) return existing as Promise<T>;

  const promise = fetchWithLock(key, ttlSeconds, fetcher)
    .finally(() => {
      inflight.delete(key);
    });

  inflight.set(key, promise);
  return promise;
}

/**
 * Like cachedFetchJson but reports the data source.
 * Use when callers need to distinguish cache hits from fresh fetches
 * (e.g. to set provider/cached metadata on responses).
 *
 * Returns { data, source } where source is:
 *   'cache'  — served from Redis
 *   'fresh'  — fetcher ran (leader) or joined an in-flight fetch (follower)
 */
export async function cachedFetchJsonWithMeta<T>(
  key: string,
  ttlSeconds: number,
  fetcher: () => Promise<T>,
): Promise<{ data: T | null; source: 'cache' | 'fresh' }> {
  const cached = await getCachedJson(key);
  if (cached !== null) return { data: cached as T, source: 'cache' };

  const existing = inflight.get(key);
  if (existing) {
    const data = (await existing) as T;
    return { data, source: 'fresh' };
  }

  const promise = fetchWithLock(key, ttlSeconds, fetcher)
    .finally(() => {
      inflight.delete(key);
    });

  inflight.set(key, promise);
  const data = await promise;
  return { data, source: 'fresh' };
}
