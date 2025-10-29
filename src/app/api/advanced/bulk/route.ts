import { NextResponse } from 'next/server';
import fs from 'fs/promises';
import path from 'path';

const DATA_DIR = path.join(process.cwd(), 'data', 'advanced');
const indexPath = path.join(DATA_DIR, 'index.json');

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

async function resolveFileForEspnId(espnId: string) {
  const direct = path.join(DATA_DIR, `${espnId}.json`);
  const d = await readJsonSafe(direct);
  if (d) return d;
  const idx = await readJsonSafe(indexPath);
  if (!idx) return null;
  // index.json can be an object keyed by espnId (sometimes produced as '3045146.0')
  if (typeof idx === 'object' && !Array.isArray(idx)) {
    // build a normalized map where keys are Number(key).toString()
    const normalized: Record<string, string> = {};
    for (const k of Object.keys(idx)) {
      const n = String(Number(k));
      normalized[n] = idx[k];
    }
    if (normalized[espnId]) {
      const p = path.join(DATA_DIR, normalized[espnId]);
      return await readJsonSafe(p);
    }
    // fallback: maybe index stored raw keys; try direct lookup as well
    if (idx[espnId]) {
      const p = path.join(DATA_DIR, idx[espnId]);
      return await readJsonSafe(p);
    }
  }
  if (Array.isArray(idx.players)) {
    const found = idx.players.find((p: any) => String(p.espnId) === String(espnId));
    if (found && found.file) {
      return await readJsonSafe(path.join(DATA_DIR, found.file));
    }
  }
  return null;
}

export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const espnParam = url.searchParams.get('espnId');
    if (!espnParam) return NextResponse.json({ ok: false, error: 'espnId required' }, { status: 400 });

    const ids = espnParam.split(',').map((s) => s.trim()).filter(Boolean);
    const cacheKey = `bulk:${ids.join(',')}`;
    const cached = cache.get(cacheKey);
    if (cached && cached.expires > Date.now()) return NextResponse.json({ ok: true, data: cached.value });

    const out: Record<string, any> = {};
    await Promise.all(ids.map(async (id) => {
      const data = await resolveFileForEspnId(id);
      if (data) out[String(id)] = data;
    }));

    cache.set(cacheKey, { expires: Date.now() + TTL_MS, value: out });
    return NextResponse.json({ ok: true, data: out });
  } catch (e) {
    return NextResponse.json({ ok: false, error: String(e) }, { status: 500 });
  }
}
