"use client";

import React, { useEffect, useState } from "react";

type PlayerRow = {
  player?: string;
  espnId?: string | null;
  [key: string]: any;
};

export default function TestAthletesPage() {
  const [rows, setRows] = useState<PlayerRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;
    fetch('/api/nfl/stocks')
      .then((r) => r.json())
      .then((json) => {
        if (!mounted) return;
        if (json && json.rows) setRows(json.rows as PlayerRow[]);
        else setError('Unexpected API response');
      })
      .catch((e) => {
        if (!mounted) return;
        setError(String(e));
      });
    return () => { mounted = false };
  }, []);

  return (
    <div className="p-4">
      <h2 className="text-xl font-semibold mb-4">Player Stocks (test)</h2>
      {error && <div className="text-red-600 mb-4">Error fetching stocks: {error}</div>}
      {!rows && !error && <div>Loading...</div>}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {rows && rows.map((r, i) => (
          <div key={i} className="border rounded p-3 shadow-sm">
            <div className="font-medium">{r.player ?? r.name ?? 'Unknown'}</div>
            <div className="text-sm text-gray-600">espnId: {r.espnId ? r.espnId : (<span className="text-red-500">❌ missing</span>)}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
