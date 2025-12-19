import React from 'react'
import PlayerStockChart from '../../../src/components/PlayerStockChart'
import PortfolioControls from '../../../src/components/PortfolioControls'
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
  let last: number | null = null
  let prev: number | null = null
  try {
    const raw = await fs.promises.readFile(histPath, 'utf8')
    const arr = JSON.parse(raw)
    if (arr && arr.length) last = Number(arr[arr.length - 1].price)
    if (arr && arr.length > 1) prev = Number(arr[arr.length - 2].price)
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
    <div className="container mx-auto p-8">
      <header className="mb-6">
        <div className="flex items-start justify-between gap-6">
          <div>
            <h1 className="m-0 text-2xl font-bold text-zinc-100">Player {playerId}</h1>
            <div className="text-zinc-400 text-sm mt-1">Name: (placeholder)</div>
          </div>

          <div className="flex items-center gap-4">
            {last != null ? (
              <>
                <div className="font-mono text-2xl font-semibold text-zinc-100">${last.toFixed(2)}</div>
                {delta != null && pct != null ? (
                  <div className={`inline-flex items-center px-3 py-1 rounded-full font-semibold ${delta > 0 ? 'bg-emerald-600 text-zinc-100' : delta < 0 ? 'bg-red-600 text-zinc-100' : 'bg-zinc-700 text-zinc-100'}`}>
                    {delta > 0 ? '▲' : delta < 0 ? '▼' : ''}
                    <span className="ml-2">{pct !== null ? `${pct > 0 ? '+' : ''}${pct.toFixed(1)}%` : ''}</span>
                  </div>
                ) : null}
              </>
            ) : (
              <div className="text-zinc-400">no price history</div>
            )}
          </div>
        </div>
      </header>

      <section>
        <div className="max-w-4xl">
          {/* PortfolioControls is a client component that manages localStorage portfolio */}
          <PortfolioControls playerId={playerId} />
          {/* PlayerStockChart is a client component that fetches /api/history/{playerId} */}
          <div className="mt-6"><PlayerStockChart playerId={playerId} /></div>
        </div>
      </section>
    </div>
  )
}

export const dynamic = 'force-dynamic'
