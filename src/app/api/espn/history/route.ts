import { NextResponse } from 'next/server';
import { filterPricePoints } from '@/lib/historyUtils';

// We need to access the same in-memory map as the price route. Since modules
// under the same server process share state, import it by referencing the
// price route module which exports the map. However, to avoid circular
// imports we'll attempt to access the map via globalThis as a fallback.

type PricePoint = { t: string; p: number };

function getGlobalHistoryMap(): Map<string, PricePoint[]> {
  // Prefer a shared symbol on globalThis
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const G: any = globalThis as any;
  if (!G.__PRICE_HISTORY_MAP__) G.__PRICE_HISTORY_MAP__ = new Map<string, PricePoint[]>();
  return G.__PRICE_HISTORY_MAP__ as Map<string, PricePoint[]>;
}

export async function GET(request: Request) {
  try {
    // If nothing in-memory, attempt to load persisted history (dev)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const maybeStore: any = (await import('@/lib/historyStore')).default;
    const map = getGlobalHistoryMap();
    if (map.size === 0 && maybeStore && typeof maybeStore.loadInto === 'function') {
      try {
        await maybeStore.loadInto(map);
      } catch (e) {
        // ignore load errors
      }
    }
    const url = new URL(request.url);
    const espnId = url.searchParams.get('espnId') || url.searchParams.get('id') || undefined;
    const name = url.searchParams.get('name') || undefined;
    if (!espnId && !name) {
      return NextResponse.json({ ok: false, error: 'expected espnId or name query param' }, { status: 400 });
    }
    const key = espnId ? String(espnId) : `name:${String(name || '').toLowerCase()}`;
  const arr = map.get(key) || [];
  const range = url.searchParams.get('range') as ('spark' | '1d' | '7d' | '30d') | null;
  const out = range ? filterPricePoints(arr, range) : arr;
  return NextResponse.json({ ok: true, espnId: espnId ?? null, name: name ?? null, history: out });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: String(err?.message ?? err) }, { status: 500 });
  }
}
