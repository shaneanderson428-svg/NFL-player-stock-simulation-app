"use client";
import React from 'react';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from 'recharts';

type Row = {
  passer_player_name?: string;
  avg_epa?: number;
  avg_cpoe?: number;
  plays?: number;
  [k: string]: any;
};

export default function LeaderboardChartInner({ data }: { data: Row[] }) {
  return (
    <div style={{ width: '100%', height: 300, marginBottom: 18 }}>
      <h2 style={{ color: '#fff', margin: '0 0 8px 0' }}>Top passers by avg EPA (chart)</h2>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} layout="vertical" margin={{ top: 10, right: 24, left: 24, bottom: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#0b1220" />
          <XAxis type="number" stroke="#9aa" />
          <YAxis dataKey="passer_player_name" type="category" width={180} stroke="#9aa" />
          <Tooltip formatter={(v: any) => (typeof v === 'number' ? v.toFixed(3) : v)} />
          <Bar dataKey="avg_epa" fill="#22c55e" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
