import { NextResponse } from 'next/server';
import { spawn } from 'child_process';
import path from 'path';
// import the flush handler so we can call it directly without an HTTP round-trip
import { POST as flushPOST } from '../flush-advanced-cache/route';
import http from 'http';
import https from 'https';
import { URL } from 'url';

export async function POST(request: Request) {
  try {
    // Only allow in non-production to avoid accidental exposure
    if (process.env.NODE_ENV === 'production') {
      return NextResponse.json({ ok: false, error: 'Not allowed in production' }, { status: 403 });
    }

    const repoRoot = process.cwd();
    const script = path.join(repoRoot, 'scripts', 'compute_advanced_metrics.py');

    const p = spawn('python3', [script, '--input', path.join(repoRoot, 'data', 'pbp'), '--output', path.join(repoRoot, 'data', 'advanced')], { stdio: ['ignore', 'pipe', 'pipe'] });

    let logs = '';
    p.stdout.on('data', (d) => {
      const s = d.toString();
      logs += s;
      // also write to server console for visibility
      console.log('[recompute-ui stdout]', s);
    });
    p.stderr.on('data', (d) => {
      const s = d.toString();
      logs += s;
      console.error('[recompute-ui stderr]', s);
    });

    const exitCode = await new Promise<number>((resolve) => p.on('close', (code: number) => resolve(Number(code ?? 0))));

    // After compute finishes, flush the in-process require cache so Next picks up new JSON files
    let flushResult: any = null;
    try {
      const res = await flushPOST();
      // Attempt to parse the NextResponse body if possible
      try {
        // @ts-ignore: NextResponse type has json method
        const body = await (res as any).json();
        flushResult = { status: (res as any).status || 200, body };
      } catch (e) {
        flushResult = { status: (res as any).status || 200 };
      }
    } catch (e: any) {
      flushResult = { error: String(e?.message ?? e) };
    }

    // Fetch the rendered /players page so the UI can show a snapshot or open the page.
    // Prefer the incoming request's Host header/protocol so we hit the same dev server instance
    // (fixes mismatches when Next dev allocates a different port).
    let playersUrl: string;
    try {
      const host = request.headers.get('host');
      const proto = request.headers.get('x-forwarded-proto') || (request.headers.get('referer')?.startsWith('https://') ? 'https' : 'http');
      if (host) {
        playersUrl = `${proto}://${host}/players`;
      } else {
        playersUrl = process.env.PLAYERS_URL || 'http://localhost:3000/players';
      }
    } catch (e) {
      playersUrl = process.env.PLAYERS_URL || 'http://localhost:3000/players';
    }
    let playersHtml: string | null = null;
    try {
      playersHtml = await (async function fetchHtml(urlStr: string) {
        const u = new URL(urlStr);
        const client = u.protocol === 'https:' ? https : http;
        return new Promise<string>((resolve, reject) => {
          const req = client.get(u, (res) => {
            let body = '';
            res.setEncoding('utf8');
            res.on('data', (c) => body += c);
            res.on('end', () => resolve(body));
          });
          req.on('error', reject);
          req.end();
        });
      })(playersUrl);
    } catch (e: any) {
      playersHtml = `Failed to fetch ${playersUrl}: ${String(e?.message ?? e)}`;
    }

    return NextResponse.json({ ok: true, exitCode, logs, flush: flushResult, playersUrl, playersHtml });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: String(e?.message ?? e) }, { status: 500 });
  }
}
