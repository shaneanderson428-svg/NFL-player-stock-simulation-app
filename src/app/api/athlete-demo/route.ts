import { NextResponse } from "next/server";
import { getAthleteStats, getPlayers } from "@/lib/api";
import { MOCK_ATHLETES } from "@/lib/mockData";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const idParam = url.searchParams.get("id");
  const id = idParam ? Number(idParam) : 14876;

  try {
    // Try the RapidAPI single-athlete endpoint first
    const data = await getAthleteStats(id);
    if (data) return NextResponse.json({ ok: true, source: "rapid", id, data });

  // If the provider failed (quota/flaky), fall back to ESPN/demo roster
  const players = await getPlayers();
  if (players) return NextResponse.json({ ok: true, source: "fallback", id, data: players });

  // As a final fallback, return the in-repo mock athletes so the frontend
  // can still render example data even when remote providers are rate-limited.
  return NextResponse.json({ ok: true, source: "mock", id, data: MOCK_ATHLETES });
  } catch (err: any) {
    console.error("/api/athlete-demo error:", err);
    // As a last resort, return a helpful error and include any fallback data
    try {
      const players = await getPlayers();
      if (players) return NextResponse.json({ ok: false, error: String(err), fallback: players }, { status: 200 });
      return NextResponse.json({ ok: false, error: String(err), fallback: MOCK_ATHLETES }, { status: 200 });
    } catch (e: any) {
      return NextResponse.json({ ok: false, error: String(err), fallback: MOCK_ATHLETES }, { status: 200 });
    }
  }
}
