import { NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

export async function POST(req: Request) {
  if (process.env.NODE_ENV === 'production') {
    return NextResponse.json({ ok: false, error: 'Upload disabled in production' }, { status: 403 });
  }
  try {
    const body = await req.json();
    const espnId = body?.espnId;
    const dataUrl = body?.dataUrl; // expected format: data:<mime>;base64,<b64>
    if (!espnId || !dataUrl) return NextResponse.json({ ok: false, error: 'missing espnId or dataUrl' }, { status: 400 });

    const match = /^data:(image\/(png|jpe?g));base64,(.*)$/i.exec(dataUrl);
    if (!match) return NextResponse.json({ ok: false, error: 'invalid dataUrl' }, { status: 400 });
    const mime = match[1];
    const ext = (match[2].toLowerCase().startsWith('png') ? 'png' : 'jpg');
    const b64 = match[3];

    const avatarsDir = path.join(process.cwd(), 'public', 'avatars');
    try { fs.mkdirSync(avatarsDir, { recursive: true }); } catch (e) {}

    const safe = String(espnId).replace(/[^a-zA-Z0-9-_\.]/g, '_');
    const outPath = path.join(avatarsDir, `${safe}.${ext}`);
    fs.writeFileSync(outPath, Buffer.from(b64, 'base64'));

    return NextResponse.json({ ok: true, url: `/avatars/${safe}.${ext}` });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: String(err?.message ?? err) }, { status: 500 });
  }
}
