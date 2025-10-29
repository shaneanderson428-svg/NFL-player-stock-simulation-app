import axios from 'axios';
import { NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

// Attempt to load sharp for safe image conversion; keep optional to avoid hard
// dependency during dev if not installed. If present, we'll convert everything
// to PNG so the public/avatars folder stays consistent.
let sharp: any = null;
try {
   
  sharp = require('sharp');
} catch (e) {
  sharp = null;
}

const CANDIDATES = [
  // ESPN combiner patterns (common). We'll try a few widths and fall back to generic.
  (id: string) => `https://a.espncdn.com/combiner/i?img=/i/headshots/nfl/players/full/${id}.png&w=256&h=256`,
  (id: string) => `https://a.espncdn.com/combiner/i?img=/i/headshots/nfl/players/full/${id}.png&w=128&h=128`,
  (id: string) => `https://a.espncdn.com/combiner/i?img=/i/headshots/nfl/players/full/${id}.png`,
];

function safeFileName(id: string) {
  return String(id).replace(/[^a-zA-Z0-9-_\.]/g, '_');
}

export async function GET(req: Request) {
  try {
    const url = new URL(req.url);
    const espnId = url.searchParams.get('espnId') || url.searchParams.get('id') || '';
    if (!espnId) return NextResponse.json({ ok: false, error: 'missing espnId' }, { status: 400 });

    const avatarsDir = path.join(process.cwd(), 'public', 'avatars');
    try { fs.mkdirSync(avatarsDir, { recursive: true }); } catch (e) { /* ignore */ }

    // Check if PNG cached file exists for espnId (enforced canonical extension)
    const base = safeFileName(espnId);
    const canonical = `${base}.png`;
    if (fs.existsSync(path.join(avatarsDir, canonical))) {
      return NextResponse.json({ ok: true, url: `/avatars/${canonical}`, cached: true });
    }

    // Try candidate URLs and download the first successful image
    for (const candidate of CANDIDATES) {
      const tryUrl = candidate(espnId);
      try {
        const resp = await axios.get(tryUrl, { responseType: 'arraybuffer', timeout: 6_000, headers: { 'User-Agent': 'my-app-avatar-fetcher/1.0' } });
        const ct = (resp.headers['content-type'] || '').toLowerCase();
        if (!ct.startsWith('image/')) continue;
        const buffer = Buffer.from(resp.data);
        const outPath = path.join(avatarsDir, canonical);
        try {
          if (sharp) {
            // Convert any incoming image to PNG for consistency.
            await sharp(buffer).png().toFile(outPath);
          } else {
            // If sharp isn't available, prefer to write original bytes when
            // they are PNG; otherwise write original bytes but still name as
            // .png only if content-type is png. If not png, write with real
            // ext as fallback and return that URL.
            if (ct.includes('png')) {
              fs.writeFileSync(outPath, buffer);
            } else {
              // Fallback: write with original ext
              const fallbackExt = ct.includes('jpeg') || ct.includes('jpg') ? 'jpg' : (ct.includes('webp') ? 'webp' : 'bin');
              const fallbackPath = path.join(avatarsDir, `${base}.${fallbackExt}`);
              fs.writeFileSync(fallbackPath, buffer);
              return NextResponse.json({ ok: true, url: `/avatars/${base}.${fallbackExt}`, cached: false });
            }
          }
          return NextResponse.json({ ok: true, url: `/avatars/${canonical}`, cached: false });
        } catch (e: any) {
          // writing/conversion failed; continue to next candidate
          console.error('avatar write failed', e?.message ?? e);
          continue;
        }
      } catch (err) {
        // Try next candidate silently
        continue;
      }
    }

    // Nothing found
    return NextResponse.json({ ok: false, error: 'no headshot found' }, { status: 404 });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: String(err?.message ?? err) }, { status: 500 });
  }
}
