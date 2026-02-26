/**
 * Vercel Cron — pre-warms the Wingbits/theater-posture Redis cache.
 *
 * Runs every minute, always fetching fresh data (bypasses cache read)
 * so user requests always hit a warm cache and never trigger upstream
 * Wingbits calls themselves.
 *
 * Requires Vercel Pro (Hobby tier limits crons to once/day).
 */

export const config = { runtime: 'edge' };

import {
  fetchTheaterPostureFresh,
  CACHE_KEY,
  CACHE_TTL,
} from '../../server/worldmonitor/military/v1/get-theater-posture';
import { setCachedJson } from '../../server/_shared/redis';

export default async function handler(request: Request): Promise<Response> {
  const authHeader = request.headers.get('authorization');
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return new Response(JSON.stringify({ error: 'Unauthorized' }), {
      status: 401,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  try {
    const result = await fetchTheaterPostureFresh();
    await setCachedJson(CACHE_KEY, result, CACHE_TTL);

    return new Response(JSON.stringify({
      ok: true,
      theaters: result.theaters?.length ?? 0,
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (err) {
    return new Response(JSON.stringify({
      ok: false,
      error: err instanceof Error ? err.message : String(err),
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}
