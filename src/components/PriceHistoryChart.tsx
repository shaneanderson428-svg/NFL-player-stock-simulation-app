"use client";
import React from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

type Props = {
  history: { week: number; price: number }[];
};

export default function PriceHistoryChart({ history }: Props) {
  if (!Array.isArray(history) || history.length === 0) {
    return null;
  }

  // Map to chart-friendly format: label weeks as W{week}
  const data = history.map((h) => ({ name: `W${h.week}`, price: Number(h.price) }));

  return (
    <div style={{ width: '100%', height: 300 }}>
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 10, right: 20, left: 0, bottom: 10 }}>
          <XAxis dataKey="name" />
          <YAxis domain={["auto", "auto"]} />
          <Tooltip />
          <Line type="monotone" dataKey="price" stroke="#22c55e" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
