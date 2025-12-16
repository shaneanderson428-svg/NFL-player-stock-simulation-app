import { NextResponse } from 'next/server';
import fs from 'fs';
import pathModule from 'path';

const CACHE_DIR = pathModule.join(process.cwd(), 'external', 'rapid', 'cache');
const DEFAULT_TTL_MS = Number(process.env.RAPID_CACHE_TTL_MS || 60_000); // default 60s

function ensureCacheDir() {
  try {
    fs.mkdirSync(CACHE_DIR, { recursive: true });
  } catch (e) {
    // ignore
  }
}

function cacheKeyFromPath(p: string) {
  // sanitize to filename
  return p.replace(/[^a-zA-Z0-9._-]/g, '_');
}

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const path = searchParams.get('path');
  if (!path) return NextResponse.json({ error: 'missing path' }, { status: 400 });

  ensureCacheDir();
  const key = cacheKeyFromPath(path);
  const cachePath = pathModule.join(CACHE_DIR, key + '.json');

  // Check filesystem cache
  try {
    if (fs.existsSync(cachePath)) {
      const raw = fs.readFileSync(cachePath, 'utf8');
      const parsed = JSON.parse(raw);
      if (parsed && parsed.ts && Date.now() - parsed.ts < DEFAULT_TTL_MS) {
        // return cached body
        return new NextResponse(JSON.stringify(parsed.body), { status: 200, headers: { 'Content-Type': 'application/json' } });
      }
    }
  } catch (e) {
    // ignore cache read errors and continue to fetch
    console.warn('Rapid proxy cache read error', e);
  }

  const base = 'https://nfl-api-data.p.rapidapi.com';
  const url = `${base}${path}`;

  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        'X-RapidAPI-Key': process.env.RAPIDAPI_KEY || '',
        'X-RapidAPI-Host': 'nfl-api-data.p.rapidapi.com',
        Accept: 'application/json',
      },
    });

    const text = await res.text();
    let body: any = null;
    try {
      body = JSON.parse(text);
    } catch (e) {
      body = text;
    }

    // Save to cache (best-effort)
    try {
      fs.writeFileSync(cachePath, JSON.stringify({ ts: Date.now(), body }), 'utf8');
    } catch (e) {
      console.warn('Rapid proxy cache write error', e);
    }

    // If rate-limited, pass through status for caller to handle
    return new NextResponse(JSON.stringify(body), { status: res.status, headers: { 'Content-Type': 'application/json' } });
  } catch (err: any) {
    console.error('Rapid proxy fetch error:', err?.message ?? err);
    return NextResponse.json({ error: String(err?.message ?? err) }, { status: 500 });
  }
}
