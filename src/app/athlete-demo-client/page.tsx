"use client";
import { useEffect, useState } from "react";
import PlayerCard from '@/app/components/PlayerCard';

export default function AthleteDemoClient() {
  const [team, setTeam] = useState<string>('all');
  const [q, setQ] = useState<string>('');
  const [page, setPage] = useState<number>(1);
  const [limit, setLimit] = useState<number>(50);

  const [players, setPlayers] = useState<any[]>([]);
  const [debug, setDebug] = useState<any>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const fetchPlayers = async (opts?: { team?: string; q?: string; page?: number; limit?: number }) => {
    const teamParam = opts?.team ?? team;
    const qParam = opts?.q ?? q;
    const pageParam = opts?.page ?? page;
    const limitParam = opts?.limit ?? limit;

    setLoading(true);
    setError(null);
    try {
      const url = `/api/espn/players?team=${encodeURIComponent(teamParam)}&page=${pageParam}&limit=${limitParam}${qParam ? `&q=${encodeURIComponent(qParam)}` : ''}`;
      const res = await fetch(url);
      const json = await res.json();
      setPlayers(Array.isArray(json.response) ? json.response : []);
      setDebug(json._debug ?? null);
    } catch (err: any) {
      setError(String(err));
      setPlayers([]);
      setDebug(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPlayers({ page, limit });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div style={{ padding: 20, fontFamily: 'Inter, system-ui, sans-serif' }}>
      <h1>Athlete Demo Client</h1>
      <p style={{ color: '#666' }}>
        This page fetches <code>/api/espn/players</code> and renders a demo client-side grid.
      </p>

      <div style={{ margin: '12px 0', display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
        <label style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          Team:
          <input value={team} onChange={(e) => setTeam(e.target.value)} style={{ padding: '8px 10px', fontSize: 16 }} />
        </label>

        <label style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          Search:
          <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="player name" style={{ padding: '8px 10px', fontSize: 16 }} />
        </label>

        <label style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          Limit:
          <input type="number" value={limit} onChange={(e) => setLimit(Number(e.target.value || 50))} style={{ width: 80, padding: '8px 10px' }} />
        </label>

        <button onClick={() => { setPage(1); fetchPlayers({ page: 1 }); }} disabled={loading} style={{ padding: '8px 12px' }}>{loading ? 'Loading…' : 'Search'}</button>
        <button onClick={() => { setQ(''); setPage(1); fetchPlayers({ q: '', page: 1 }); }} style={{ padding: '8px 12px' }}>Reset</button>
      </div>

      {error && (
        <div style={{ color: 'crimson', marginBottom: 12 }}>Error: {error}</div>
      )}

      <div style={{ marginTop: 12 }}>
        <div style={{ marginBottom: 8 }}>
          <strong>Players (page {page})</strong>
        </div>

        {loading ? (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 12 }}>
            {Array.from({ length: Math.min(6, limit) }).map((_, i) => (
              <div key={i} style={{ background: '#071025', padding: 12, borderRadius: 10, height: 88 }} />
            ))}
          </div>
        ) : players.length > 0 ? (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 12 }}>
            {players.map((p) => (
              <div key={p.id ?? p.name}>
                <PlayerCard player={p} />
              </div>
            ))}
          </div>
        ) : (
          <div>No players found.</div>
        )}

        <div style={{ marginTop: 12, display: 'flex', gap: 8, alignItems: 'center' }}>
          <button disabled={page <= 1 || loading} onClick={() => { const next = Math.max(1, page - 1); setPage(next); fetchPlayers({ page: next }); }} style={{ padding: '8px 12px' }}>Prev</button>
          <button disabled={loading} onClick={() => { const next = page + 1; setPage(next); fetchPlayers({ page: next }); }} style={{ padding: '8px 12px' }}>Next</button>
          <span style={{ color: '#9aa' }}>
            {debug ? `normalizedCount: ${debug.normalizedCount ?? 'unknown'}${debug.cacheHit ? ' (cache)' : ''}` : ''}
          </span>
        </div>

        {debug && (
          <div style={{ marginTop: 12, color: '#bbb' }}>
            <h3>Debug info</h3>
            <pre style={{ color: '#ccc', whiteSpace: 'pre-wrap' }}>{JSON.stringify(debug, null, 2)}</pre>
          </div>
        )}
      </div>
    </div>
  );
}

