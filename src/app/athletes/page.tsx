// NOTE: temporarily avoid importing client components (PlayerCard / TeamSection)
// For interactive rendering of player cards we dynamically import the client
// `TeamSection` component with SSR disabled so the server page can remain
// a server component while client-only UI mounts on the browser.
// Import client component directly; Next will handle the client boundary.
import TeamSection from '@/app/components/TeamSection';
// here to isolate a runtime hydration error. The page will render simple
// server-side summaries until the client components are investigated.
import { headers } from 'next/headers';
import { MOCK_ATHLETES } from '@/lib/mockData';

type Props = any;

export default async function AthletesPage({ searchParams }: Props) {
  // `searchParams` can be a sync or async value in different Next versions;
  // await it to satisfy the server runtime expectation.
  const sp = (await (searchParams ?? {})) as Record<string, string | undefined>;
  const page = Math.max(1, Number(sp?.page ?? 1));
  const limit = Math.min(200, Math.max(10, Number(sp?.limit ?? 50)));

  // Fetch aggregated player stocks from our new stocks API which returns
  // `{ ok, rows, players, teams }`. We prefer `players` (consumer-friendly)
  // which includes blended stock and basic fields.
  const reqHeaders = await headers();
  const host = reqHeaders.get('host') ?? 'localhost:3000';
  const proto = reqHeaders.get('x-forwarded-proto') ?? 'http';
  const base = `${proto}://${host}`;
  const url = new URL(`/api/nfl/stocks?`, base).toString();
  let res: Response | null = null;
  let stocksResp: any = null;
  try {
    res = await fetch(url, { next: { revalidate: 60 } });
    try {
      stocksResp = await res.json();
    } catch (errJson) {
      console.error('[athletes/page] failed to parse /api/nfl/stocks JSON', { url, err: String(errJson) });
      stocksResp = null;
    }
  } catch (err) {
    console.error('[athletes/page] error fetching /api/nfl/stocks', String(err));
    stocksResp = null;
  }

  // If API failed, fall back to mock athletes so the page is still usable.
  const playersList: any[] = (stocksResp && Array.isArray(stocksResp.players) && stocksResp.players.length > 0)
    ? stocksResp.players
    : MOCK_ATHLETES;

  const hasPrev = page > 1;
  const hasNext = false;

  return (
    <div style={{ padding: '20px', color: 'white' }}>
      <h1>All NFL Players</h1>

      <div style={{ margin: '12px 0', display: 'flex', gap: 12, alignItems: 'center' }}>
        <div>
          <a href={`?page=${Math.max(1, page - 1)}&limit=${limit}`} style={{ opacity: hasPrev ? 1 : 0.4, pointerEvents: hasPrev ? 'auto' : 'none' }}>◀ Prev</a>
        </div>
        <div>Page {page}</div>
        <div>
          <a href={`?page=${page + 1}&limit=${limit}`} style={{ opacity: hasNext ? 1 : 0.4, pointerEvents: hasNext ? 'auto' : 'none' }}>Next ▶</a>
        </div>
      </div>

      {res && !res.ok && (
        <p style={{ color: '#faa' }}>Error fetching players: {res.status} {res.statusText}</p>
      )}

      {playersList && playersList.length > 0 ? (
        // Group players by team for a scrollable dashboard
        (() => {
          const map = new Map<string, any[]>();
          for (const p of playersList) {
            const team = (p.team || '').toString() || 'No Team';
            if (!map.has(team)) map.set(team, []);
            map.get(team)!.push(p);
          }
          // Sort teams alphabetically, with 'No Team' last
          const teams = Array.from(map.keys()).sort((a, b) => {
            if (a === 'No Team') return 1;
            if (b === 'No Team') return -1;
            return a.localeCompare(b);
          });
          return (
            <div>
              {teams.map((t) => (
                // Render the client-side TeamSection dynamically (no SSR)
                <TeamSection key={t} teamName={t} players={(map.get(t) || [])} />
              ))}
            </div>
          );
        })()
      ) : (
        <p>No players found (provider returned empty or an error).</p>
      )}

      {stocksResp?._debug && (
        <div style={{ marginTop: 20, color: '#bbb' }}>
          <h3>Debug info</h3>
          <pre style={{ color: '#ccc' }}>{JSON.stringify(stocksResp._debug, null, 2)}</pre>
        </div>
      )}
    </div>
  );
}
