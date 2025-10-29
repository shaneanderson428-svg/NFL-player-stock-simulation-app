import { NextResponse } from 'next/server';
import historyStore from '@/lib/historyStore';

type ResultItem = {
  espnId: string | number;
  price?: number; // absolute price to set
  deltaPct?: number; // fractional change to apply to last price (e.g. 0.1 = +10%)
};

export async function POST(req: Request) {
  try {
    const body = await req.json();
    const items: ResultItem[] = body?.results || [];
    if (!Array.isArray(items) || items.length === 0) {
      return NextResponse.json({ ok: false, error: 'expected { results: [{ espnId, price?|deltaPct? }, ...] }' }, { status: 400 });
    }

    // Load current persisted map to compute last prices if needed
    const map = await historyStore.loadMap();

    const now = new Date().toISOString();
    const ops: Array<Promise<any>> = [];

    for (const it of items) {
      const key = String(it.espnId);
      const arr = map.get(key) || [];
      const last = arr[arr.length - 1];
      let newPrice: number | null = null;
      if (typeof it.price === 'number') {
        newPrice = it.price;
      } else if (typeof it.deltaPct === 'number') {
        const base = last?.p ?? Number(body?.fallbackBase ?? 100);
        newPrice = Math.max(0.01, +(base * (1 + it.deltaPct)).toFixed(2));
      } else {
        // nothing to do for this item
        continue;
      }

      const point = { t: now, p: Number(newPrice) };
      ops.push(historyStore.appendPoint(key, point));
    }

    await Promise.all(ops);
    return NextResponse.json({ ok: true, appended: items.length });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: String(e?.message ?? e) }, { status: 500 });
  }
}
