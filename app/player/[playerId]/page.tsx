import React from 'react'
import PlayerStockChart from '../../../src/components/PlayerStockChart'
import PortfolioControls from '../../../src/components/PortfolioControls'
import OwnedSharesBadge from '../../../src/components/OwnedSharesBadge'
import fs from 'fs'
import path from 'path'

type Props = {
  params: Promise<{ playerId: string }>
}

export default async function Page(props: Props) {
  const { playerId } = await props.params

  // Attempt to read last two history entries for this player (prefer public/history copied at build)
  const repoRoot = process.cwd()
  const publicHist = path.join(repoRoot, 'public', 'history', `${playerId}_price_history.json`)
  const dataHist = path.join(repoRoot, 'data', 'history', `${playerId}_price_history.json`)
  const histPath = fs.existsSync(publicHist) ? publicHist : dataHist
  // Attempt to read player metadata from player_stock_summary.csv (prefer public copy)
  let displayName: string | null = null
  let teamName: string | null = null
  let positionName: string | null = null
  try {
    const publicCsv = path.join(repoRoot, 'public', 'player_stock_summary.csv')
    const dataCsv = path.join(repoRoot, 'data', 'player_stock_summary.csv')
    const csvPath = fs.existsSync(publicCsv) ? publicCsv : dataCsv
    if (fs.existsSync(csvPath)) {
      const rawCsv = await fs.promises.readFile(csvPath, 'utf8')
      const lines = rawCsv.split(/\r?\n/).filter(Boolean)
      if (lines.length > 0) {
        const header = lines[0].split(',')
        const idxEspn = header.indexOf('espnId')
        const idxName = header.indexOf('player')
        const idxTeam = header.indexOf('team')
        const idxPos = header.indexOf('position')
        if (idxEspn >= 0) {
          for (let i = 1; i < lines.length; i++) {
            const parts = lines[i].split(',')
            const espn = parts[idxEspn]
            if (String(espn) === String(playerId)) {
              if (idxName >= 0) displayName = parts[idxName]
              if (idxTeam >= 0) teamName = parts[idxTeam]
              if (idxPos >= 0) positionName = parts[idxPos]
              break
            }
          }
        }
      }
    }
  } catch (err) {
    // ignore
  }
  let last: number | null = null
  let prev: number | null = null
  let lastReason: string | null = null
  let lastEntry: any = null
  try {
    const raw = await fs.promises.readFile(histPath, 'utf8')
    const arr = JSON.parse(raw)
    if (arr && arr.length) {
      lastEntry = arr[arr.length - 1]
      last = Number(lastEntry.price ?? lastEntry.close ?? null)
    }
    if (arr && arr.length > 1) prev = Number(arr[arr.length - 2].price ?? arr[arr.length - 2].close ?? null)
    if (lastEntry) lastReason = lastEntry.reason || null
  } catch (err) {
    // ignore missing history
  }

  let delta: number | null = null
  let pct: number | null = null
  if (last != null && prev != null) {
    delta = last - prev
    pct = prev !== 0 ? (delta / prev) * 100 : null
  }

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100">
      <div className="container mx-auto p-8">
        <header className="mb-6">
          <div className="flex flex-col md:flex-row items-start md:items-center justify-between gap-4">
            <div>
              <h1 className="m-0 text-3xl md:text-4xl font-extrabold text-zinc-100">{`Player ${playerId}`}</h1>
              <div className="text-zinc-400 text-sm mt-1">{lastReason ? `Last update: ${lastReason}` : ''}</div>
            </div>

            <div className="flex items-center gap-4">
              {last != null ? (
                <>
                  <div className="text-sm text-zinc-400">Current Price</div>
                  <div className="flex items-center">
                    <div className="font-mono text-2xl md:text-3xl font-semibold text-zinc-100">${last.toFixed(2)}</div>
                    <OwnedSharesBadge playerId={playerId} />
                  </div>
                  {delta != null && pct != null ? (
                    <div className={`inline-flex items-center px-3 py-1 rounded-full font-semibold ${delta > 0 ? 'bg-emerald-600 text-zinc-100' : delta < 0 ? 'bg-red-600 text-zinc-100' : 'bg-zinc-700 text-zinc-100'}`}>
                      {delta > 0 ? '▲' : delta < 0 ? '▼' : ''}
                      <span className="ml-2">{pct !== null ? `${pct > 0 ? '+' : ''}${pct.toFixed(1)}%` : ''}</span>
                    </div>
                  ) : null}
                </>
              ) : (
                <div className="text-zinc-400">No price history</div>
              )}
            </div>
          </div>
        </header>

        <main className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <aside className="md:col-span-1">
            <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-4 space-y-4">
              <h2 className="text-lg font-semibold">Overview</h2>
              <div className="text-zinc-400 text-sm">Player ID: <span className="text-zinc-200 font-mono">{playerId}</span></div>
              {displayName && <div className="text-zinc-400 text-sm">Name: <span className="text-zinc-200">{displayName}</span></div>}
              {teamName && <div className="text-zinc-400 text-sm">Team: <span className="text-zinc-200">{teamName}</span></div>}
              {positionName && <div className="text-zinc-400 text-sm">Position: <span className="text-zinc-200">{positionName}</span></div>}

              <div>
                <h3 className="text-sm text-zinc-400 mb-2">Portfolio</h3>
                <PortfolioControls playerId={playerId} />
              </div>
            </div>
          </aside>

          <section className="md:col-span-2">
            <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-4">
              <PlayerStockChart playerId={playerId} />
            </div>
          </section>
        </main>
      </div>
    </div>
  )
}

export const dynamic = 'force-dynamic'
