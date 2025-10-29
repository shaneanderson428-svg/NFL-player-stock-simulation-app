import { NextResponse } from 'next/server';

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const path = searchParams.get('path');
  if (!path) return NextResponse.json({ error: 'missing path' }, { status: 400 });

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
    return new NextResponse(text, { status: res.status, headers: { 'Content-Type': 'application/json' } });
  } catch (err: any) {
    return NextResponse.json({ error: String(err?.message ?? err) }, { status: 500 });
  }
}
