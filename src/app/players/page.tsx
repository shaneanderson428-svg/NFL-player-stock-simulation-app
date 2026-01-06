import React from 'react';
import PlayerCard from '@/app/components/PlayerCard';
import PlayersVirtualGrid from './PlayersVirtualGrid';
import LeaderboardChart from '@/app/components/LeaderboardChart';
import { renderSparkline as renderSparklineShared } from '@/lib/sparkline';
import historyStore from '@/lib/historyStore';
import path from 'path';
import fs from 'fs/promises';

export const revalidate = 10;

async function readJson(p: string) {
  try {
    const raw = await fs.readFile(p, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

export default async function Page({ searchParams }: { searchParams?: { position?: string; all?: string; page?: string; limit?: string } }) {
  const DATA_DIR = path.join(process.cwd(), 'data', 'advanced');
  const index = await readJson(path.join(DATA_DIR, 'index.json'));
  // index.json can be either { players: [{ espnId, file }, ...], lastUpdated: "..." }
  // or a flat map produced by the compute script: { "3045146.0": "3045146.0.json", ... }
  let playersIndex: Array<{ espnId: number; file: string }> = [];
  if (index) {
    if (Array.isArray(index.players)) {
      playersIndex = index.players;
    } else if (typeof index === 'object' && !Array.isArray(index)) {
      // Detect flat-map shape (keys are espnIds, values are filenames)
      const keys = Object.keys(index);
      const isFlatMap = keys.length > 0 && typeof index[keys[0]] === 'string';
      if (isFlatMap) {
        playersIndex = keys.map((k) => ({ espnId: Number(k), file: String(index[k]) }));
      }
    }
  }

  const histMap = await historyStore.loadMap();

  // Load generated player metadata map (optional). This map is generated from
  // weekly CSVs by scripts/generate_player_meta.py and helps ensure every
  // playerId has a name/position/team to display on the players page.
  const PLAYER_META_PATH = path.join(process.cwd(), 'data', 'player_meta.json');
  const playerMetaMap = (await readJson(PLAYER_META_PATH)) || {};

  // Build a small quick map from any price CSVs: playerId -> { name, position, team }
  // This helps when advanced JSONs are missing or player_meta doesn't contain names.
  const PRICE_BASE = path.join(process.cwd(), 'data', 'prices');
  const pricePlayerMap: Record<string, { name?: string; position?: string; team?: string }> = {};
  try {
    const years = await fs.readdir(PRICE_BASE).catch(() => []);
    for (const y of years || []) {
      const dir = path.join(PRICE_BASE, String(y));
      try {
        const files = await fs.readdir(dir).catch(() => []);
        for (const f of files || []) {
          if (!f.endsWith('.csv')) continue;
          const fp = path.join(dir, f);
          try {
            const raw = await fs.readFile(fp, 'utf8');
            const lines = raw.split(/\r?\n/).filter(Boolean);
            if (lines.length < 2) continue;
            const hdr = lines[0].split(',').map(h => h.trim());
            const nameIdx = hdr.findIndex(h => /playername|player_name|name/i.test(h));
            const idIdx = hdr.findIndex(h => /playerid|espnid|id/i.test(h));
            const posIdx = hdr.findIndex(h => /position|pos/i.test(h));
            const teamIdx = hdr.findIndex(h => /team|team_abbr|teamabbr/i.test(h));
            if (idIdx < 0) continue;
            for (let i = 1; i < lines.length; i++) {
              const cols = lines[i].split(',');
              const pid = String((cols[idIdx] || '').trim());
              if (!pid) continue;
              const existing = pricePlayerMap[pid] || {};
              if (!existing.name && nameIdx >= 0) existing.name = (cols[nameIdx] || '').trim() || undefined;
              if (!existing.position && posIdx >= 0) existing.position = (cols[posIdx] || '').trim() || undefined;
              if (!existing.team && teamIdx >= 0) existing.team = (cols[teamIdx] || '').trim() || undefined;
              pricePlayerMap[pid] = existing;
            }
          } catch (e) {
            // ignore malformed price CSV
          }
        }
      } catch (e) {
        // ignore
      }
    }
  } catch (e) {
    // ignore
  }

  // Load team map file (optional). If present, it should be a map of espnId -> { abbreviation, name }
  const TEAM_MAP_PATH = path.join(process.cwd(), 'data', 'team-map.json');
  const teamMap = (await readJson(TEAM_MAP_PATH)) || {};

  // Load each advanced file
  const items = await Promise.all(playersIndex.map(async (p) => {
    const fp = path.join(DATA_DIR, p.file || `${p.espnId}.json`);
    const d = await readJson(fp);
    const espnId = String(p.espnId || d?.espnId || '');
    const persisted = histMap.get(espnId) || histMap.get(String(Number(espnId))) || null;
    const team = d?.team || teamMap[String(p.espnId)] || null;
    // Merge generated player meta as a fallback, then price CSV map
    const pm = playerMetaMap[String(espnId)] || playerMetaMap[String(Number(espnId))] || {};
    const pp = pricePlayerMap[String(espnId)] || {};
    const fallbackName = pm?.name || null;
    const fallbackPos = pm?.position || null;
    const fallbackTeam = pm?.team || null;
    const priceName = pp?.name || null;
    const pricePos = pp?.position || null;
    const priceTeam = pp?.team || null;
    return {
      espnId,
      name: d?.player ?? d?.playerName ?? d?.player ?? fallbackName ?? priceName,
      position: d?.position ?? fallbackPos ?? pricePos ?? null,
      team: d?.team ?? team ?? fallbackTeam ?? priceTeam ?? null,
      advanced: d?.metrics ?? d ?? null,
      persistedHistory: Array.isArray(persisted) ? persisted : null,
      raw: d ?? null,
    };
  }));

  // Optional position filter from querystring, e.g. /players?position=WR
  // `searchParams` can be treated as async in some Next.js versions; resolve it first
  const sp = await Promise.resolve(searchParams);
  const positionFilter = String((sp?.position) || '').trim().toUpperCase();

  // If caller requested the full roster (all=1) then fetch the API server-side
  // and render those players instead of the advanced index. This enables
  // viewing all 700+ players (and their priceHistory) e.g. /players?all=1&position=WR
  const allParam = String((sp?.all) || '').trim().toLowerCase();
  let filteredItems = items;
  if (allParam === '1' || allParam === 'true' || allParam === 'yes') {
    try {
      // fetch the API server-side — Next's fetch to a relative path works in server components
      const posQuery = positionFilter ? `&position=${encodeURIComponent(positionFilter)}` : '';
      const res = await fetch(`/api/nfl/stocks?all=1${posQuery}`, { cache: 'no-store' });
      const json = await res.json();
      const players = Array.isArray(json?.players) ? json.players : [];
      // Map API players into the same shape expected by the PlayerCard mapping below
      filteredItems = players.map((p: any) => ({
        espnId: String(p.espnId || p.id || ''),
        name: p.name || p.player || '',
        position: p.position || p.position_profile || '',
        team: p.team || '',
        raw: p || null,
        // include server-side priceHistory so PlayerCard can render sparklines immediately
        persistedHistory: Array.isArray(p.priceHistory) ? p.priceHistory : (Array.isArray(p.history) ? p.history : []),
        // also expose stock/confidence for immediate display via PlayerCard prop synth
        stock: p.stock,
        confidence: p.confidence,
        history: Array.isArray(p.history) ? p.history : undefined,
      }));
    } catch (e) {
      // on failure fall back to advanced index items
      filteredItems = items;
    }
  } else {
    filteredItems = positionFilter
      ? items.filter((it) => String(it.position || '').toUpperCase() === positionFilter)
      : items;
  }

  // MAIN PLAYERS LIST FILTER: remove players that have NO resolved name AND a
  // current price below $40. This only applies to the main (advanced) list —
  // when the page is rendering the inline `items` array (not the full-roster
  // `all=1` virtualized view which fetches via API).
  const isFullRosterView = (allParam === '1' || allParam === 'true' || allParam === 'yes');
  if (!isFullRosterView) {
    const extractLastPrice = (it: any): number | null => {
      try {
        const hist = Array.isArray(it.persistedHistory) && it.persistedHistory.length ? it.persistedHistory : (Array.isArray(it.raw?.priceHistory) ? it.raw.priceHistory : (Array.isArray(it.raw?.history) ? it.raw.history : []));
        if (!Array.isArray(hist) || hist.length === 0) return null;
        const last = hist[hist.length - 1] || {};
        const v = last.price ?? last.close ?? last.p ?? last.stock ?? last.price_close ?? null;
        const n = v == null || v === '' ? null : Number(v);
        return Number.isFinite(n) ? n : null;
      } catch (e) {
        return null;
      }
    };

    filteredItems = filteredItems.filter((it) => {
      const name = it.name ?? it.raw?.player ?? it.raw?.playerName ?? '';
      const hasName = name !== null && name !== undefined && String(name).trim() !== '';
      if (hasName) return true;
      const price = extractLastPrice(it);
      // Only filter out when we have a numeric price and it's below the threshold.
      if (price !== null && price < 40) return false;
      return true;
    });
  }

  // Default sort: when rendering the main players list, show highest-priced
  // players first (descending by last known price). Do not affect the
  // full-roster client-side virtualized view.
  if (!isFullRosterView) {
    const getLastPriceForSort = (it: any): number => {
      try {
        const hist = Array.isArray(it.persistedHistory) && it.persistedHistory.length ? it.persistedHistory : (Array.isArray(it.raw?.priceHistory) ? it.raw.priceHistory : (Array.isArray(it.raw?.history) ? it.raw.history : []));
        if (!Array.isArray(hist) || hist.length === 0) return 0;
        const last = hist[hist.length - 1] || {};
        const v = last.price ?? last.close ?? last.p ?? last.stock ?? last.price_close ?? null;
  const n = v == null || v === '' ? null : Number(v);
  const num = n === null ? NaN : Number(n);
  return Number.isFinite(num) ? num : 0;
      } catch (e) {
        return 0;
      }
    };

    filteredItems.sort((a: any, b: any) => {
      return getLastPriceForSort(b) - getLastPriceForSort(a);
    });
  }

  // Compute Top Movers (Top 5 Gainers and Top 5 Losers) based on
  // last history entry's `price_change_pct` where available. This will be
  // displayed above the main players list as a compact list.
  const extractLastChange = (it: any): number | null => {
    try {
      const hist = Array.isArray(it.persistedHistory) && it.persistedHistory.length
        ? it.persistedHistory
        : (Array.isArray(it.raw?.priceHistory) ? it.raw.priceHistory : (Array.isArray(it.raw?.history) ? it.raw.history : []));
      if (!Array.isArray(hist) || hist.length === 0) return null;
      const last = hist[hist.length - 1] || {};
      // Common field names we've used: price_change_pct, price_change, change_pct
      const v = last.price_change_pct ?? last.price_change ?? last.change_pct ?? null;
      if (v !== null && v !== undefined && v !== '') {
        const n = Number(v);
        return Number.isFinite(n) ? n : null;
      }

      // Fallback: try to compute from last two prices in history
      if (hist.length >= 2) {
        const prev = hist[hist.length - 2] || {};
        const p1 = last.price ?? last.close ?? last.p ?? null;
        const p2 = prev.price ?? prev.close ?? prev.p ?? null;
        const n1 = (p1 === null || p1 === '') ? null : Number(p1);
        const n2 = (p2 === null || p2 === '') ? null : Number(p2);
        if (n1 !== null && n2 !== null) {
          const nn1 = Number(n1);
          const nn2 = Number(n2);
          if (Number.isFinite(nn1) && Number.isFinite(nn2) && Math.abs(nn2) > 1e-9) {
            return (nn1 - nn2) / Math.abs(nn2);
          }
        }
      }
      return null;
    } catch (e) {
      return null;
    }
  };

  const movers = filteredItems.map((it) => ({
    espnId: it.espnId,
    name: it.name ?? it.raw?.player ?? it.raw?.playerName ?? 'Unknown',
    position: it.position,
    team: it.team,
    lastChange: extractLastChange(it),
    lastPrice: (() => {
      try {
        const hist = Array.isArray(it.persistedHistory) && it.persistedHistory.length ? it.persistedHistory : (Array.isArray(it.raw?.priceHistory) ? it.raw.priceHistory : (Array.isArray(it.raw?.history) ? it.raw.history : []));
        if (!Array.isArray(hist) || hist.length === 0) return null;
        const last = hist[hist.length - 1] || {};
        const v = last.price ?? last.close ?? last.p ?? null;
        const n = v == null || v === '' ? null : Number(v);
        return Number.isFinite(n) ? n : null;
      } catch (e) {
        return null;
      }
    })(),
    raw: it,
  }));

  const withChange = movers.filter(m => m.lastChange !== null && Number.isFinite(m.lastChange));
  const topGainers = [...withChange].sort((a, b) => (b.lastChange as number) - (a.lastChange as number)).slice(0, 5);
  const topLosers = [...withChange].sort((a, b) => (a.lastChange as number) - (b.lastChange as number)).slice(0, 5);

  // Helper to pick a color for the change indicator: green for positive,
  // red for negative, gray for near-zero or missing. Threshold is 0.5%.
  const changeColorFor = (v: number | null) => {
    if (v === null || !Number.isFinite(v)) return '#9ca3af'; // gray-400
    const abs = Math.abs(v);
    if (abs < 0.005) return '#9ca3af'; // treat <0.5% as near-zero
    return v > 0 ? '#4ade80' : '#ef4444'; // green-400 or red-500
  };

  const changeTooltip = 'Price moved based on recent performance, expectations, and market momentum.';

  // Dev-only: warn when a player will render without a name after all fallbacks.
  // This logs to the server console in non-production environments and does
  // not affect the rendered UI.
  if (process.env.NODE_ENV !== 'production') {
    try {
      for (const it of filteredItems) {
        const name = it.name ?? it.raw?.player ?? it.raw?.playerName ?? '';
        if (!String(name).trim()) {
          // include espnId so it's easy to trace the missing metadata
          // eslint-disable-next-line no-console
          console.warn(`[DEV] Player rendering without name after fallbacks: espnId=${it.espnId}`);
        }
      }
    } catch (e) {
      // swallow any issues in the dev-only diagnostic
    }
  }

  return (
    <div style={{ padding: 18 }}>
      <div className="players-header">
        <div>
          <h1 style={{ color: '#fff', margin: 0 }}>All advanced players</h1>
        </div>
      </div>

      <LeaderboardChart minPlays={50} topN={10} />

      <div style={{ marginTop: 12, marginBottom: 8, position: 'relative', zIndex: 20 }}>
        <a href="/players" className={`player-meta ${(!positionFilter && allParam !== '1') ? 'active' : ''}`} style={{ marginRight: 12, pointerEvents: 'auto' }}>Advanced</a>
        <a href="/players?all=1" className={`player-meta ${(allParam === '1' && !positionFilter) ? 'active' : ''}`} style={{ marginRight: 12, pointerEvents: 'auto' }}>Full roster</a>
        <a href="/players?all=1&position=WR" className={`player-meta ${(positionFilter === 'WR' && allParam === '1') ? 'active' : ''}`} style={{ marginRight: 12, pointerEvents: 'auto' }}>WRs (all)</a>
        <a href="/players?all=1&position=RB" className={`player-meta ${(positionFilter === 'RB' && allParam === '1') ? 'active' : ''}`} style={{ marginRight: 12, pointerEvents: 'auto' }}>RBs (all)</a>
        <a href="/players?all=1&position=TE" className={`player-meta ${(positionFilter === 'TE' && allParam === '1') ? 'active' : ''}`} style={{ marginRight: 12, pointerEvents: 'auto' }}>TEs (all)</a>
        <a href="/players?position=QB" className={`player-meta ${(positionFilter === 'QB' && allParam !== '1') ? 'active' : ''}`} style={{ marginRight: 12, pointerEvents: 'auto' }}>QBs (advanced)</a>
        <span className="player-meta">Showing {filteredItems.length} players{positionFilter ? ` (position=${positionFilter})` : ''}</span>
      </div>

      {/* Top Movers section (compact) */}
      <div style={{ display: 'flex', gap: 16, marginTop: 8, marginBottom: 12 }}>
        <div style={{ flex: 1, background: 'rgba(255,255,255,0.03)', padding: 10, borderRadius: 6 }}>
          <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 8 }}>Top 5 Gainers</div>
          {topGainers.length === 0 ? (
            <div style={{ color: '#999', fontSize: 13 }}>No recent movers</div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {topGainers.map((m) => (
                <a key={m.espnId} href={`/players/${m.espnId}`} style={{ display: 'flex', justifyContent: 'space-between', textDecoration: 'none', color: 'inherit', padding: '6px 4px', borderRadius: 4 }}>
                  <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 240 }}>{m.name}</div>
                  <div style={{ display: 'flex', gap: 12, alignItems: 'center', fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, monospace' }}>
                    <div style={{ color: '#fff' }}>{m.lastPrice !== null ? `$${Number(m.lastPrice).toFixed(2)}` : '—'}</div>
                    <div title={changeTooltip} aria-label={changeTooltip} style={{ color: changeColorFor(m.lastChange) }}>{(Number(m.lastChange) * 100).toFixed(1)}%</div>
                  </div>
                </a>
              ))}
            </div>
          )}
        </div>
        <div style={{ flex: 1, background: 'rgba(255,255,255,0.03)', padding: 10, borderRadius: 6 }}>
          <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 8 }}>Top 5 Losers</div>
          {topLosers.length === 0 ? (
            <div style={{ color: '#999', fontSize: 13 }}>No recent movers</div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {topLosers.map((m) => (
                <a key={m.espnId} href={`/players/${m.espnId}`} style={{ display: 'flex', justifyContent: 'space-between', textDecoration: 'none', color: 'inherit', padding: '6px 4px', borderRadius: 4 }}>
                  <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 240 }}>{m.name}</div>
                  <div style={{ display: 'flex', gap: 12, alignItems: 'center', fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, monospace' }}>
                    <div style={{ color: '#fff' }}>{m.lastPrice !== null ? `$${Number(m.lastPrice).toFixed(2)}` : '—'}</div>
                    <div title={changeTooltip} aria-label={changeTooltip} style={{ color: changeColorFor(m.lastChange) }}>{(Number(m.lastChange) * 100).toFixed(1)}%</div>
                  </div>
                </a>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* If this page was rendered with the `all=1` query we prefer a virtualized client grid
          to avoid rendering 700+ cards during server-side hydration. The PlayersVirtualGrid
          client component uses react-window and accepts the same minimal player shapes. */}
      { (allParam === '1' || allParam === 'true' || allParam === 'yes') ? (
        // For the full-roster view we intentionally let the client component fetch
        // the roster itself to avoid serializing a large players array into the
        // initial HTML. This reduces hydration risk and improves perceived load.
        <PlayersVirtualGrid />
      ) : (
          <div>
            {/* Header row for main players list */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '8px 0', marginBottom: 8 }}>
              <div style={{ fontSize: 13, fontWeight: 700 }}>Player</div>
              <div style={{ display: 'flex', gap: 24, alignItems: 'center' }}>
                <div style={{ fontSize: 13, fontWeight: 700, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, monospace' }}>Price</div>
                <div style={{ fontSize: 13, fontWeight: 700, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, monospace' }}>Change</div>
              </div>
            </div>
            <div className="players-grid grid gap-4 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
          {filteredItems.map((p) => (
            <div key={p.espnId}>
              <a href={`/players/${p.espnId}`} aria-label={`Open ${p.name ?? 'player'} details`}>
              <PlayerCard player={{
                id: p.espnId,
                name: p.name ?? p.raw?.player ?? 'Unknown',
                position: p.position,
                espnId: p.espnId,
                team: p.team,
                // priceHistory / history: prefer persistedHistory (from API) then raw
                priceHistory: p.persistedHistory ?? p.raw?.priceHistory ?? [],
                // expose computed stock/confidence/history so PlayerCard can show stock immediately
                stock: (p as any).stock,
                confidence: (p as any).confidence,
                history: (p as any).history ?? (p as any).persistedHistory ?? p.raw?.history,
                // keep raw payload for debugging
                _raw: p.raw ?? p.raw,
              }} />
              </a>
            </div>
          ))}
        </div>
          </div>
      )}
    </div>
  );
}
