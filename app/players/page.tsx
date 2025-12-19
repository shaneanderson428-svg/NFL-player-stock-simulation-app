import fs from 'fs'
import path from 'path'
import Link from 'next/link'
import React from 'react'

export default async function PlayersPage() {
  const repoRoot = process.cwd()
  const publicHistoryDir = path.join(repoRoot, 'public', 'history')
  const dataHistoryDir = path.join(repoRoot, 'data', 'history')
  const dir = fs.existsSync(publicHistoryDir) ? publicHistoryDir : dataHistoryDir
  let files: string[] = []
  try {
    files = await fs.promises.readdir(dir)
  } catch (err) {
    files = []
  }

  const suffix = '_price_history.json'

  // Attempt to load player names from player_stock_summary.csv (prefer public copy generated at build)
  const nameMap: Record<string, string> = {}
  try {
    const publicCsv = path.join(repoRoot, 'public', 'player_stock_summary.csv')
    const dataCsv = path.join(repoRoot, 'data', 'player_stock_summary.csv')
    const csvPath = fs.existsSync(publicCsv) ? publicCsv : dataCsv
    const raw = await fs.promises.readFile(csvPath, 'utf8')
    const lines = raw.split(/\r?\n/).filter(Boolean)
    if (lines.length > 0) {
      const header = lines[0].split(',')
      const idxName = header.indexOf('player')
      const idxEspn = header.indexOf('espnId')
      if (idxName >= 0 && idxEspn >= 0) {
        for (let i = 1; i < lines.length; i++) {
          const parts = lines[i].split(',')
          const espn = parts[idxEspn]
          const nm = parts[idxName]
          if (espn && nm) nameMap[String(espn)] = nm
        }
      }
    }
  } catch (err) {
    // ignore; fallback will show ids only
  }

  const playerIds = files
    .filter((f) => f.endsWith(suffix))
    .map((f) => f.slice(0, -suffix.length))
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))

  // Read last two entries from each player's history to compute change
  const players = await Promise.all(
    playerIds.map(async (id) => {
      const histPath = path.join(dir, `${id}${suffix}`)
      try {
        const raw = await fs.promises.readFile(histPath, 'utf8')
        const arr = JSON.parse(raw)
        const last = arr && arr.length ? Number(arr[arr.length - 1].price) : null
        const prev = arr && arr.length > 1 ? Number(arr[arr.length - 2].price) : null
        let delta: number | null = null
        let pct: number | null = null
        if (last != null && prev != null) {
          delta = last - prev
          pct = prev !== 0 ? (delta / prev) * 100 : null
        }
        return {
          id,
          name: nameMap[id] || null,
          last,
          prev,
          delta,
          pct,
        }
      } catch (err) {
        return { id, name: nameMap[id] || null, last: null, prev: null, delta: null, pct: null }
      }
    })
  )
  // (dir already determined above)

  return (
    <div className="container mx-auto p-8">
      <h1 className="text-2xl font-bold text-zinc-100 mb-6">Players</h1>
      {/* What changed / recent updates */}
      <div className="mb-6">
        <div className="bg-gradient-to-b from-zinc-900 to-zinc-950 border border-zinc-800 rounded-lg p-4">
          <div className="text-sm text-zinc-400 mb-1">Recent changes</div>
          <ul className="list-disc list-inside text-zinc-200 text-sm">
            <li>Chart tooltip improved — shows Open / High / Low / Close (OHLC) on hover with monospaced prices.</li>
            <li>Added a <Link href="/portfolio" className="underline">/portfolio</Link> page — aggregates your localStorage portfolio, cash, holdings and P&L.</li>
            <li>Portfolio controls and UI polished to the dark trading theme (monospaced price display).</li>
          </ul>
        </div>
      </div>
      <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-4">
        <ul className="divide-y divide-zinc-800">
          {players.map((p) => (
            <li key={p.id} className="py-3 flex items-center justify-between">
              <div className="flex items-center gap-4">
                <Link href={`/player/${p.id}`} className="text-zinc-100 font-semibold">
                  {p.name ? `${p.name}` : p.id}
                </Link>
                <div className="text-zinc-400 text-sm">{p.name ? `(${p.id})` : ''}</div>
              </div>

              <div className="flex items-center gap-3">
                {p.last != null ? (
                  <>
                    <div className="font-mono text-zinc-100">${p.last.toFixed(2)}</div>
                    {p.delta != null && p.pct != null ? (
                      <div className={`text-sm font-semibold inline-flex items-center px-3 py-1 rounded-full ${p.pct > 0 ? 'bg-emerald-600 text-zinc-100' : p.pct < 0 ? 'bg-red-600 text-zinc-100' : 'bg-zinc-600 text-zinc-100'}`}>
                        {p.pct > 0 ? '▲' : p.pct < 0 ? '▼' : ''}
                        <span className="ml-2">{p.pct !== null ? `${p.pct > 0 ? '+' : ''}${p.pct.toFixed(1)}%` : ''}</span>
                      </div>
                    ) : null}
                  </>
                ) : (
                  <div className="text-zinc-400">no history</div>
                )}
              </div>
            </li>
          ))}
        </ul>
      </div>
    </div>
  )
}

export const dynamic = 'force-dynamic'
