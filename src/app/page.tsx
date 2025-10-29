import PlayerStatsWrapper from "./PlayerStatsWrapper";

export default function HomePage() {
  return (
    <main style={{ padding: 24, color: "white" }}>
      <h1>Welcome</h1>
      <p>This is a minimal root page to satisfy Next.js type generation.</p>
      <div style={{ marginTop: 24 }}>
        <PlayerStatsWrapper />
      </div>
    </main>
  );
}
