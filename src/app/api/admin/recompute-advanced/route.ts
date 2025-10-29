import { NextResponse } from 'next/server';
import { spawn } from 'child_process';
import path from 'path';

export async function POST(req: Request) {
  try {
    // Guard: require a secret header to avoid accidental public triggering.
    const secretHeader = (req.headers.get('x-advanced-secret') || '').trim();
    const expected = process.env.ADVANCED_SECRET || '';
    if (!expected || secretHeader !== expected) {
      return NextResponse.json({ ok: false, error: 'Missing or invalid secret' }, { status: 401 });
    }

    // best-effort: call the Python compute script to regenerate data/advanced
    const repoRoot = process.cwd();
    const script = path.join(repoRoot, 'scripts', 'compute_advanced_metrics.py');
    // Spawn a child process and stream output to the Next log (dev only)
    const p = spawn('python3', [script, '--input', path.join(repoRoot, 'data', 'pbp'), '--output', path.join(repoRoot, 'data', 'advanced')], { stdio: ['ignore', 'pipe', 'pipe'] });

    p.stdout.on('data', (d) => console.log('[compute stdout]', d.toString()));
    p.stderr.on('data', (d) => console.error('[compute stderr]', d.toString()));

    const exit = await new Promise((resolve) => p.on('close', resolve));
    return NextResponse.json({ ok: true, exitCode: Number(exit) });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: String(e?.message ?? e) }, { status: 500 });
  }
}
