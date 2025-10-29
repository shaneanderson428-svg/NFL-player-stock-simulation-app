import { NextResponse } from "next/server";
import { getPlayers } from "@/lib/api";
import { MOCK_ATHLETES } from "@/lib/mockData";

export async function GET(req: Request) {
  try {
    const url = new URL(req.url);
    const team = url.searchParams.get("team");

    if (team) {
      // Try ESPN roster path via getPlayers(team). getPlayers prefers ESPN for team queries
      try {
        const resp = await getPlayers(team);
        // If getPlayers returned a wrapper { response, _debug } like elsewhere, unwrap
        if (resp && typeof resp === 'object' && 'response' in resp) return NextResponse.json(resp);
        if (Array.isArray(resp)) return NextResponse.json({ ok: true, source: 'espn', response: resp });
      } catch (e) {
        // fallthrough to mock
      }
    }

    // Default: return mock athletes
    return NextResponse.json({ ok: true, source: 'mock', response: MOCK_ATHLETES });
  } catch (err: any) {
    console.error('/api/fallback error', err);
    return NextResponse.json({ ok: false, error: String(err) }, { status: 500 });
  }
}
