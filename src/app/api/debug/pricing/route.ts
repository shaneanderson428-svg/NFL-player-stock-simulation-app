import { NextResponse } from 'next/server';
import { POSITION_MULTIPLIERS, POSITION_WEIGHTS } from '@/lib/pricing';

export async function GET() {
  if (process.env.NODE_ENV === 'production') {
    return NextResponse.json({ error: 'disabled in production' }, { status: 403 });
  }
  return NextResponse.json({ ok: true, multipliers: POSITION_MULTIPLIERS, weights: POSITION_WEIGHTS });
}
