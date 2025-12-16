"use client";
import React from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

// Client-side chart that receives a single flattened row (latest game) and
// renders a simple bar chart for receptions, yards, targets, fantasy points.

type Props = {
  row: Record<string, any>;
};

function _num(v: any) {
  if (v == null) return 0;
  const n = Number(String(v).replace(/[^0-9.-]/g, ''));
  return Number.isFinite(n) ? n : 0;
}

export default function WRChart({ row }: Props) {
  // Try several common flattened key names
  const receptions = _num(row['Receiving.receptions'] ?? row['receiving.receptions'] ?? row.receptions ?? row.rec ?? row.Rec ?? 0);
  const targets = _num(row['Receiving.targets'] ?? row['receiving.targets'] ?? row.targets ?? row.target ?? 0);
  const yards = _num(row['Receiving.recYds'] ?? row['receiving.recYds'] ?? row.yards ?? row.yds ?? 0);
  const fantasy = _num(row['fantasyPoints'] ?? row['fantasyPoints.total'] ?? row.fantasy ?? row.fantasyPointsDefault?.standard ?? row.fantasyPointsDefault ?? 0);

  const data = [
    { name: 'Receptions', value: receptions },
    { name: 'Yards', value: yards },
    { name: 'Targets', value: targets },
    { name: 'Fantasy', value: fantasy },
  ];

  // advanced metrics may be present on the row or as last history point
  const lastHist = Array.isArray(row?.priceHistory) && row.priceHistory.length ? row.priceHistory[row.priceHistory.length - 1] : null;
  const pis = _num(row.pis ?? lastHist?.pis ?? 0);
  const its = _num(row.its ?? lastHist?.its ?? 0);
  // include PIS and ITS as additional bars (different scale, but informative)
  data.push({ name: 'PIS', value: pis });
  data.push({ name: 'ITS', value: its });

  return (
    <div style={{ width: '100%', height: 320 }}>
      <ResponsiveContainer>
        <BarChart data={data} margin={{ top: 20, right: 30, left: 0, bottom: 20 }}>
          <XAxis dataKey="name" />
          <YAxis />
          <Tooltip />
          <Bar dataKey="value" fill="#8884d8" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
