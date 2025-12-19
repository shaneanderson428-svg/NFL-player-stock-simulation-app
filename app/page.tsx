import Link from 'next/link'
import React from 'react'
import fs from 'fs'
import path from 'path'

export default async function HomePage() {
  // Build a name map from player_stock_summary.csv if available (prefer public copy)
  const nameMap: Record<string, string> = {}
  try {
    const repoRoot = process.cwd()
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
    // ignore
  }

  // Compute weekly percent changes for all players and pick top 5
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
  const movers: Array<{ id: string; name?: string | null; last: number | null; prev: number | null; pct: number | null }> = []
  for (const f of files) {
    if (!f.endsWith(suffix)) continue
    const id = f.slice(0, -suffix.length)
    try {
      const raw = await fs.promises.readFile(path.join(dir, f), 'utf8')
      const arr = JSON.parse(raw)
      const last = arr && arr.length ? Number(arr[arr.length - 1].price) : null
      const prev = arr && arr.length > 1 ? Number(arr[arr.length - 2].price) : null
      let pct: number | null = null
      if (last != null && prev != null && prev !== 0) pct = ((last - prev) / prev) * 100
      movers.push({ id, name: nameMap[id] || null, last, prev, pct })
    } catch (err) {
      // skip malformed
    }
  }

  movers.sort((a, b) => {
    const ap = a.pct ?? -Infinity
    const bp = b.pct ?? -Infinity
    return bp - ap
  })

  const top5 = movers.slice(0, 5)

  
  return (
    <main className="min-h-screen bg-zinc-950 text-zinc-100 flex items-center">
      <div className="container mx-auto px-6 py-24">
        <div className="max-w-4xl mx-auto text-center">
          <h1 className="text-4xl sm:text-5xl md:text-6xl font-extrabold leading-tight">
            Trade Professional Athletes Like Stocks
          </h1>
          <p className="mt-6 text-zinc-400 text-lg sm:text-xl">
            Bring performance-based value to athlete markets — track per-game performance, watch prices move with results, and manage your portfolio with confidence.
          </p>

          <div className="mt-10 flex justify-center gap-4">
            <Link href="/players" className="inline-block bg-emerald-500 hover:bg-emerald-400 text-zinc-950 font-semibold py-3 px-6 rounded-lg shadow-lg transform transition-all duration-200 hover:-translate-y-0.5">
              Sign In
            </Link>
            <a
              href="#features"
              className="inline-block border border-zinc-800 text-zinc-200 hover:border-zinc-700 hover:text-zinc-100 py-3 px-5 rounded-lg"
            >
              Learn More
            </a>
          </div>
        </div>
 
        {/* Top Movers This Week */}
        <section className="mt-12 max-w-4xl mx-auto">
          <h2 className="text-xl font-semibold mb-4">Top Movers This Week</h2>
          <div className="bg-zinc-900 border border-zinc-800 p-4 rounded-xl">
            {top5.length === 0 ? (
              <div className="text-zinc-400">No movers available</div>
            ) : (
              <ul className="space-y-2">
                {top5.map((p) => (
                  <li key={p.id} className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <div className="w-10 text-sm text-zinc-400">{p.name ? p.name : p.id}</div>
                      <Link href={`/player/${p.id}`} className="text-zinc-100 font-medium hover:underline">
                        {p.name ? `${p.name}` : p.id}
                      </Link>
                    </div>

                    <div className="flex items-center gap-3">
                      <div className="text-zinc-400 text-sm font-mono">{p.last != null ? `$${p.last.toFixed(2)}` : '—'}</div>
                      <div
                        className={`text-sm font-semibold inline-flex items-center px-3 py-1 rounded-full ${
                          p.pct != null ? (p.pct > 0 ? 'bg-emerald-600 text-zinc-100' : p.pct < 0 ? 'bg-red-600 text-zinc-100' : 'bg-zinc-500 text-zinc-100') : 'bg-zinc-700 text-zinc-400'
                        }`}
                      >
                        {p.pct != null ? (p.pct > 0 ? '▲' : p.pct < 0 ? '▼' : '') : ''}
                        <span className="ml-2">{p.pct != null ? `${p.pct > 0 ? '+' : ''}${p.pct.toFixed(1)}%` : '—'}</span>
                      </div>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </section>

        <section id="features" className="mt-16">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <article className="bg-gradient-to-b from-gray-800 to-gray-700 p-6 rounded-2xl shadow-sm hover:shadow-lg transform transition-all duration-200 hover:-translate-y-1">
              <div className="flex items-center space-x-4">
                <div className="p-3 bg-emerald-600 rounded-md">
                  <svg className="w-6 h-6 text-white" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M3 3v18h18" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                    <path d="M21 3l-6 6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </div>
                <div>
                  <h3 className="text-lg font-semibold">Real-Time Trading</h3>
                  <p className="mt-1 text-gray-300 text-sm">Buy and sell athlete positions as performance updates arrive.</p>
                </div>
              </div>
            </article>

            <article className="bg-gradient-to-b from-gray-800 to-gray-700 p-6 rounded-2xl shadow-sm hover:shadow-lg transform transition-all duration-200 hover:-translate-y-1">
              <div className="flex items-center space-x-4">
                <div className="p-3 bg-emerald-600 rounded-md">
                  <svg className="w-6 h-6 text-white" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M12 3v18" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                    <path d="M3 12h18" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </div>
                <div>
                  <h3 className="text-lg font-semibold">Performance Based</h3>
                  <p className="mt-1 text-gray-300 text-sm">Prices reflect real game performance — value moves with results.</p>
                </div>
              </div>
            </article>

            <article className="bg-gradient-to-b from-gray-800 to-gray-700 p-6 rounded-2xl shadow-sm hover:shadow-lg transform transition-all duration-200 hover:-translate-y-1">
              <div className="flex items-center space-x-4">
                <div className="p-3 bg-emerald-600 rounded-md">
                  <svg className="w-6 h-6 text-white" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M4 12h16" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                    <path d="M4 6h16" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                    <path d="M4 18h16" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </div>
                <div>
                  <h3 className="text-lg font-semibold">Track Your Portfolio</h3>
                  <p className="mt-1 text-gray-300 text-sm">Monitor holdings, historical performance, and price trends in one place.</p>
                </div>
              </div>
            </article>
          </div>
        </section>
      </div>
    </main>
  )
}

export const dynamic = 'force-dynamic'
