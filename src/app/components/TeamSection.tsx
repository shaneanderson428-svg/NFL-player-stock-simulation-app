"use client";
import React from 'react';
import PlayerCard from './PlayerCard';

type Props = {
  teamName: string;
  players: any[];
};

export default function TeamSection({ teamName, players }: Props) {
  return (
    <section style={{ marginBottom: 28 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
        <h2 style={{ margin: 0 }}>{teamName || 'No Team'}</h2>
        <div style={{ fontSize: 13, color: '#9aa' }}>{players.length} players</div>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 12 }}>
        {players.map((p) => (
          <PlayerCard key={p.id || p.espnId || p.name} player={p} />
        ))}
      </div>
    </section>
  );
}
