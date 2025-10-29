"use client";

import React, { useMemo } from "react";
import useSWR from "swr";
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid } from "recharts";

const fetcher = (url: string) => fetch(url).then((r) => r.json());

type StockRow = {
  player: string;
  week: number;
  stock: number;
  confidence?: number;
  [k: string]: any;
};

function simplify(s?: string) {
  return (s || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function initialLast(name?: string) {
  if (!name) return "";
  const parts = name.trim().split(/\s+/);
  if (parts.length === 1) return simplify(name);
  return (parts[0][0] + parts.slice(-1)[0]).toLowerCase();
}

function lastName(name?: string) {
  if (!name) return "";
  const parts = name.trim().split(/\s+/);
  return parts.slice(-1)[0].toLowerCase();
}

export default function PlayerStatsChart({ defaultPlayer }: { defaultPlayer?: string | null }) {
  const { data, error } = useSWR("/api/nfl/stocks", fetcher, { refreshInterval: 15000 });

  const rows: StockRow[] = useMemo(() => {
    if (!data) return [];
    const raw = data.players || data.rows || data.data || data;
    if (!Array.isArray(raw)) return [];
    return raw
      .map((r: any) => ({
        player: String(r.player || r.name || "").trim(),
        week: Number(r.week ?? r.week_number ?? r.w ?? 0),
        stock: Number(r.stock ?? r.score ?? 0),
        confidence: r.confidence != null ? Number(r.confidence) : undefined,
        ...r,
      }))
      .filter((r) => r.player);
  }, [data]);

  const playerName = defaultPlayer || "";

  const matched = useMemo(() => {
    if (!playerName || rows.length === 0) return [] as StockRow[];
    const s = simplify(playerName);

    // 1) exact normalized
    let found = rows.filter((r) => simplify(r.player) === s);
    if (found.length) return found;

    // 2) initial+last
    const il = initialLast(playerName);
    found = rows.filter((r) => initialLast(r.player) === il);
    if (found.length) return found;

    // 3) last name match
    const ln = lastName(playerName);
    found = rows.filter((r) => lastName(r.player) === ln || simplify(r.player).includes(ln));
    if (found.length) return found;

    // 4) substring
    found = rows.filter((r) => simplify(r.player).includes(s) || s.includes(simplify(r.player)));
    return found;
  }, [playerName, rows]);

  if (error) return <div>Failed to load chart data</div>;
  if (!data) return <div>Loading chart…</div>;
  if (!matched || matched.length === 0) return <div>No chart data for {defaultPlayer ?? "player"}</div>;

  const series = matched
    .slice()
    .sort((a, b) => Number(a.week) - Number(b.week))
    .map((r) => ({ week: Number(r.week), stock: Number(r.stock) }));

  const first = series[0]?.stock ?? 0;
  const last = series[series.length - 1]?.stock ?? first;
  const stroke = last >= first ? "#22c55e" : "#ef4444";

  return (
    <div style={{ width: "100%", height: 260 }}>
      <ResponsiveContainer>
        <LineChart data={series}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="week" />
          <YAxis domain={["dataMin", "dataMax"]} />
          <Tooltip />
          <Line type="monotone" dataKey="stock" stroke={stroke} dot={{ r: 3 }} strokeWidth={2} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
