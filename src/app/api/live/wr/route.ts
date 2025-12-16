import fs from 'fs/promises';
import path from 'path';
import { NextResponse } from 'next/server';

const HOST = 'tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com';
const BASE = `https://${HOST}`;
const TRY_ENDPOINTS = [
  '/getNFLPlayerLiveGameStats',
  '/getNFLBoxScore',
  '/getNFLPlayerInfo',
  '/getPlayerList?position=WR',
];

function safeJson(text: string) {
  try {
    return text ? JSON.parse(text) : null;
  } catch (e) {
    return text;
  }
}

function normalizePlayers(raw: any): any[] {
  if (!raw) return [];
  let players: any[] = [];
  if (Array.isArray(raw)) players = raw as any[];
  else if (Array.isArray(raw.players)) players = raw.players;
  else if (Array.isArray(raw.data)) players = raw.data;
  else if (Array.isArray(raw.items)) players = raw.items;
  else if (raw && typeof raw === 'object') {
    const first = Object.values(raw).find((v: any) => Array.isArray(v));
    if (Array.isArray(first)) players = first;
  }

  return players.map((p: any) => {
    const id = p?.id ?? p?.playerId ?? p?.playerID ?? p?.espnId ?? p?.espnid ?? p?.player_id ?? p?.identifier ?? null;
    const name = p?.name ?? p?.player ?? p?.longName ?? p?.fullName ?? p?.displayName ?? '';
    const team = p?.team ?? p?.teamAbbr ?? p?.teamName ?? p?.school ?? '';
    const position = p?.pos ?? p?.position ?? p?.positionCode ?? '';
    const status = p?.status ?? p?.gameStatus ?? p?.injuryStatus ?? '';

    let stats = p?.stats ?? p?.gameStats ?? {};
    if (!stats || Object.keys(stats).length === 0) {
      stats = {};
      for (const [k, v] of Object.entries(p || {})) {
        if (['id', 'playerId', 'name', 'player', 'team', 'position', 'status'].includes(k)) continue;
        if (typeof v === 'number') stats[k] = v;
        if (typeof v === 'string' && v !== '' && !isNaN(Number(v))) stats[k] = Number(v);
      }
    }

    return { id: id == null ? '' : String(id), name: String(name), team: String(team), position: String(position), stats, status: String(status) };
  });
}

export async function GET(request: Request) {
  const key = process.env.RAPIDAPI_KEY || process.env.NEXT_PUBLIC_RAPIDAPI_KEY;
  if (!key) {
    return NextResponse.json({ ok: false, error: 'RAPIDAPI_KEY not configured' }, { status: 500 });
  }

  const u = new URL(request.url);
  const season = u.searchParams.get('season');
  const week = u.searchParams.get('week');
  const gameId = u.searchParams.get('gameId') || u.searchParams.get('game_id');
  const positionParam = u.searchParams.get('position');

  const outDir = path.join(process.cwd(), 'external', 'tank01');
  await fs.mkdir(outDir, { recursive: true });

  const headers = {
    'X-RapidAPI-Key': key,
    'X-RapidAPI-Host': HOST,
    Accept: 'application/json',
  };

  for (const epRaw of TRY_ENDPOINTS) {
    try {
      let url = epRaw.startsWith('/') ? BASE + epRaw : BASE + '/' + epRaw;
      const hasQS = url.includes('?');
      const params: string[] = [];
      if (season) params.push(`season=${encodeURIComponent(season)}`);
      if (week) params.push(`week=${encodeURIComponent(week)}`);
      if (gameId) params.push(`gameId=${encodeURIComponent(gameId)}`);
      if (positionParam) params.push(`position=${encodeURIComponent(positionParam)}`);
      if (params.length) url = url + (hasQS ? '&' : '?') + params.join('&');

      const res = await fetch(url, { method: 'GET', headers });
      const text = await res.text().catch(() => '');
      const data = safeJson(text);

      if (!res.ok) {
        try {
          const errPath = path.join(outDir, 'live_wr_error.json');
          await fs.writeFile(errPath, JSON.stringify({ ts: new Date().toISOString(), endpoint: url, status: res.status, body: data }, null, 2), 'utf8');
        } catch (err) {
          // swallow
        }
        continue;
      }

      const players = normalizePlayers(data);
      try {
        const outPath = path.join(outDir, 'live_wr.json');
        await fs.writeFile(outPath, JSON.stringify({ ts: new Date().toISOString(), endpoint: url, players, raw: data }, null, 2), 'utf8');
      } catch (err) {
        // ignore write failures
      }

      return NextResponse.json({ status: 'ok', endpoint: url, count: players.length, sample: players[0] ?? null });
    } catch (e: any) {
      try {
        const errPath = path.join(outDir, 'live_wr_error.json');
        await fs.writeFile(errPath, JSON.stringify({ ts: new Date().toISOString(), endpoint: epRaw, error: String(e?.message ?? e) }, null, 2), 'utf8');
      } catch (err) {
        // swallow
      }
      continue;
    }
  }

  return NextResponse.json({ status: 'error', error: 'no working endpoint found' }, { status: 502 });
}

