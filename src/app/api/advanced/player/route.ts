import { NextResponse } from 'next/server';
import fs from 'fs/promises';
import path from 'path';

const DATA_DIR = path.join(process.cwd(), 'data', 'advanced');
const indexPath = path.join(DATA_DIR, 'index.json');

// Simple in-memory TTL cache
const cache = new Map<string, { expires: number; value: any }>();
const TTL_MS = Number(process.env.ADVANCED_STATS_TTL_MS ?? 60 * 60 * 1000); // 1 hour

async function readJsonSafe(p: string) {
  try {
    const raw = await fs.readFile(p, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const espnId = url.searchParams.get('espnId');
    if (!espnId) return NextResponse.json({ ok: false, error: 'espnId required' }, { status: 400 });

    const cached = cache.get(String(espnId));
    if (cached && cached.expires > Date.now()) return NextResponse.json({ ok: true, data: cached.value });

    // Try direct file
    const filePath = path.join(DATA_DIR, `${espnId}.json`);
    const data = await readJsonSafe(filePath);
    if (data) {
      cache.set(String(espnId), { expires: Date.now() + TTL_MS, value: data });
      return NextResponse.json({ ok: true, data });
    }

    // Fallback to index lookup - support either a keyed map or an array "players"
    const idx = await readJsonSafe(indexPath);
    if (idx) {
      // If index is a map keyed by espnId
      if (idx[espnId]) {
        const p = path.join(DATA_DIR, idx[espnId]);
        const d = await readJsonSafe(p);
        if (d) {
          cache.set(String(espnId), { expires: Date.now() + TTL_MS, value: d });
          return NextResponse.json({ ok: true, data: d });
        }
      }

      // If index has an array of players: { players: [{ espnId, file }, ...] }
      if (Array.isArray(idx.players)) {
        const found = idx.players.find((p: any) => String(p.espnId) === String(espnId));
        if (found && found.file) {
          const pth = path.join(DATA_DIR, found.file);
          const d = await readJsonSafe(pth);
          if (d) {
            cache.set(String(espnId), { expires: Date.now() + TTL_MS, value: d });
            return NextResponse.json({ ok: true, data: d });
          }
        }
      }
    }

    return NextResponse.json({ ok: false, error: 'no advanced stats found' }, { status: 404 });
  } catch (e) {
    return NextResponse.json({ ok: false, error: String(e) }, { status: 500 });
  }
}
