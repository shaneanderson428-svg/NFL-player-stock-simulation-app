import { NextResponse } from 'next/server';

function parseFloatSafe(v: any) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const espnId = url.searchParams.get('espnId');
    const startPrice = Number(url.searchParams.get('startPrice') ?? 100);
    if (!espnId) return NextResponse.json({ ok: false, error: 'expected espnId' }, { status: 400 });

    // Query our local players endpoint to get priceHistory. Use the request origin
    // so this works regardless of the dev server port (3000 or 3001).
    const origin = url.origin || `${request.headers.get('x-forwarded-proto') || 'http'}://${request.headers.get('host')}`;
    const playersRes = await fetch(`${origin}/api/espn/players?team=all&limit=500`);
    const playersJson = await playersRes.json().catch(() => ({}));
    const players = playersJson?.response || [];
    const pl = players.find((p: any) => String(p.id) === String(espnId) || String(p.espnId) === String(espnId));
    if (!pl) return NextResponse.json({ ok: false, error: 'player not found' }, { status: 404 });

    const rawHistory = Array.isArray(pl.priceHistory) ? pl.priceHistory : [];
    if (rawHistory.length === 0) return NextResponse.json({ ok: false, error: 'no priceHistory available for player' }, { status: 404 });

    // Normalize: sort by time and compute percent deltas relative to previous recorded p
    const sorted = [...rawHistory].sort((a, b) => new Date(a.t).getTime() - new Date(b.t).getTime());
    const series: Array<{ t: string; p: number }> = [];
    let cur = Number(startPrice) || 100;
    let prevRecorded: number | null = null;
    for (const rec of sorted) {
      const recP = parseFloatSafe(rec.p);
      if (recP == null) continue;
      if (prevRecorded == null) {
        // compute ratio between recorded first point and desired startPrice
        const ratio = recP > 0 ? (cur / recP) : 1;
        // scale doesn't change day-to-day percent changes
        series.push({ t: rec.t, p: Number(cur.toFixed(2)) });
        prevRecorded = recP;
        continue;
      }
      // percent change from prevRecorded to recP
      const pct = prevRecorded > 0 ? (recP - prevRecorded) / prevRecorded : 0;
      cur = Math.max(0.01, Number((cur * (1 + pct)).toFixed(2)));
      series.push({ t: rec.t, p: cur });
      prevRecorded = recP;
    }

    return NextResponse.json({ ok: true, espnId, startPrice: Number(startPrice), series });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: String(err?.message ?? err) }, { status: 500 });
  }
}

