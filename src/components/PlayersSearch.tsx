"use client"

import React, { useMemo, useState } from 'react'
import Link from 'next/link'

type PlayerRow = {
  id: string
  name?: string | null
  team?: string | null
  position?: string | null
  last?: number | null
  pct?: number | null
}

export default function PlayersSearch({ initialPlayers }: { initialPlayers: PlayerRow[] }) {
  const [q, setQ] = useState('')
  const [posFilter, setPosFilter] = useState('')
  const [teamFilter, setTeamFilter] = useState('')

  const positions = useMemo(() => {
    const s = new Set<string>()
    initialPlayers.forEach((p) => p.position && s.add(p.position))
    return Array.from(s).sort()
  }, [initialPlayers])

  const teams = useMemo(() => {
    const s = new Set<string>()
    initialPlayers.forEach((p) => p.team && s.add(p.team))
    return Array.from(s).sort()
  }, [initialPlayers])

  const filtered = useMemo(() => {
    const qq = q.trim().toLowerCase()
    return initialPlayers.filter((p) => {
      if (posFilter && (p.position || '') !== posFilter) return false
      if (teamFilter && (p.team || '') !== teamFilter) return false
      if (!qq) return true
      return (
        (p.name || '').toLowerCase().includes(qq) ||
        (p.id || '').toLowerCase().includes(qq) ||
        (p.team || '').toLowerCase().includes(qq) ||
        (p.position || '').toLowerCase().includes(qq)
      )
    }).slice(0, 100)
  }, [initialPlayers, q, posFilter, teamFilter])

  return (
    <div>
      <div className="mb-4 flex gap-3 items-center">
        <input aria-label="Search players" value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search by name, id, team or position" className="flex-1 py-2 px-3 rounded-md bg-zinc-900 border border-zinc-800 text-zinc-100" />
        <select value={posFilter} onChange={(e) => setPosFilter(e.target.value)} className="py-2 px-3 rounded-md bg-zinc-900 border border-zinc-800 text-zinc-100">
          <option value="">All positions</option>
          {positions.map((p) => <option key={p} value={p}>{p}</option>)}
        </select>
        <select value={teamFilter} onChange={(e) => setTeamFilter(e.target.value)} className="py-2 px-3 rounded-md bg-zinc-900 border border-zinc-800 text-zinc-100">
          <option value="">All teams</option>
          {teams.map((t) => <option key={t} value={t}>{t}</option>)}
        </select>
      </div>

      <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-2">
        {filtered.length === 0 ? (
          <div className="p-4 text-zinc-400">No players match your search</div>
        ) : (
          <ul className="divide-y divide-zinc-800">
            {filtered.map((p) => (
              <li key={p.id} className="py-3 px-2 flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <Link href={`/player/${p.id}`} className="font-semibold text-zinc-100 hover:underline">
                    {p.name ? p.name : p.id}
                  </Link>
                  <div className="text-zinc-400 text-sm">{p.team || '—'}</div>
                  <div className="text-zinc-400 text-sm">{p.position || '—'}</div>
                </div>

                <div className="flex items-center gap-3">
                  <div className="font-mono text-zinc-100">{p.last != null ? `$${p.last.toFixed(2)}` : '—'}</div>
                  {p.pct != null ? (
                    <div className={`text-sm font-semibold inline-flex items-center px-3 py-1 rounded-full ${p.pct > 0 ? 'bg-emerald-600 text-zinc-100' : p.pct < 0 ? 'bg-red-600 text-zinc-100' : 'bg-zinc-600 text-zinc-100'}`}>
                      {p.pct > 0 ? '▲' : p.pct < 0 ? '▼' : ''}
                      <span className="ml-2">{p.pct !== null ? `${p.pct > 0 ? '+' : ''}${p.pct.toFixed(1)}%` : ''}</span>
                    </div>
                  ) : null}
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  )
}
