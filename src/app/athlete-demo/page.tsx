import { getAthleteStats } from "@/lib/api";

export default async function AthleteDemoPage({ searchParams }: any) {
  // allow overriding the demo id via ?id= on the URL for inspection
  const id = Number(searchParams?.id ?? 14876);
  const data = await getAthleteStats(id);

  return (
    <div style={{ padding: 20, color: "white" }}>
      <h1>Athlete Demo: {id}</h1>
      {data ? (
        <pre style={{ whiteSpace: "pre-wrap", color: "#ddd" }}>
          {JSON.stringify(data, null, 2)}
        </pre>
      ) : (
        <div style={{ color: "red" }}>Failed to load athlete data. Check server logs.</div>
      )}
    </div>
  );
}
