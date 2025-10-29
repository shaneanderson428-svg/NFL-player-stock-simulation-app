import { NextResponse } from 'next/server';
import { getCacheStats } from '@/lib/cache';

export async function GET() {
  if (process.env.NODE_ENV === 'production') {
    return NextResponse.json({ error: 'disabled in production' }, { status: 403 });
  }
  try {
    const stats = getCacheStats();
    return NextResponse.json({ ok: true, stats });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: String(err?.message ?? err) }, { status: 500 });
  }
}
