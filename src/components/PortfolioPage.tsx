"use client"

import React, { useEffect, useState } from 'react'

type Portfolio = {
  cash: number
  positions: Record<string, { shares: number; avgPrice?: number }>
}

type Holding = {
  playerId: string
  shares: number
  avgPrice?: number
  lastPrice?: number
  value?: number
}

const STORAGE_KEY = 'nfl_portfolio_v1'

export default function PortfolioPage() {
  const [portfolio, setPortfolio] = useState<Portfolio | null>(null)
  const [holdings, setHoldings] = useState<Holding[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const raw = typeof window !== 'undefined' ? localStorage.getItem(STORAGE_KEY) : null
    let pf: Portfolio | null = null
    try {
      pf = raw ? JSON.parse(raw) : null
    } catch (e) {
      pf = null
    }
    if (!pf) {
      // create default
      pf = { cash: 10000, positions: {} }
      try { localStorage.setItem(STORAGE_KEY, JSON.stringify(pf)) } catch (e) {}
    }
    setPortfolio(pf)
  }, [])

  useEffect(() => {
    if (!portfolio) return
    const ids = Object.keys(portfolio.positions)
    if (ids.length === 0) {
      setHoldings([])
      return
    }
    setLoading(true)
    setError(null)
    Promise.all(ids.map(async (id) => {
      try {
        const res = await fetch(`/api/history/${encodeURIComponent(id)}`)
        if (!res.ok) return { playerId: id, shares: portfolio.positions[id].shares, avgPrice: portfolio.positions[id].avgPrice } as Holding
        const json = await res.json()
        const arr = Array.isArray(json) ? json.slice().sort((a: any, b: any) => (a.week || 0) - (b.week || 0)) : []
        const last = arr.length ? arr[arr.length - 1].price : undefined
        const shares = portfolio.positions[id].shares
        const avgPrice = portfolio.positions[id].avgPrice
        const value = last != null ? shares * last : undefined
        return { playerId: id, shares, avgPrice, lastPrice: last, value }
      } catch (err: any) {
        return { playerId: id, shares: portfolio.positions[id].shares, avgPrice: portfolio.positions[id].avgPrice } as Holding
      }
    })).then((hs) => {
      setHoldings(hs)
    }).catch((err) => {
      setError(String(err?.message || err))
    }).finally(() => setLoading(false))
  }, [portfolio])

  const totalPositions = holdings.reduce((s, h) => s + (h.value || 0), 0)
  const cash = portfolio ? portfolio.cash : 0
  const total = cash + totalPositions

  return (
    <div className="p-6">
      <h2 className="text-2xl font-semibold mb-4 text-zinc-100">Portfolio</h2>
      {!portfolio && <div className="text-zinc-400">Loading portfolio…</div>}
      {portfolio && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-4">
            <div className="text-sm text-zinc-400">Cash</div>
            <div className="font-mono text-xl text-zinc-100">${portfolio.cash.toFixed(2)}</div>
            <div className="mt-3 text-sm text-zinc-400">Positions Value</div>
            <div className="font-mono text-xl text-zinc-100">${totalPositions.toFixed(2)}</div>
            <div className="mt-3 text-sm text-zinc-400">Total</div>
            <div className="font-mono text-2xl text-zinc-100">${total.toFixed(2)}</div>
          </div>

          <div className="md:col-span-2 bg-zinc-900 border border-zinc-800 rounded-lg p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="text-sm text-zinc-400">Holdings</div>
              <div className="text-sm text-zinc-400">{loading ? 'Refreshing…' : ''}</div>
            </div>
            {error && <div className="text-red-400 mb-2">{error}</div>}
            {holdings.length === 0 && <div className="text-zinc-400">No positions.</div>}
            {holdings.length > 0 && (
              <div className="space-y-3">
                {holdings.map((h) => {
                  const pl = h.lastPrice != null && h.avgPrice != null ? (h.lastPrice - h.avgPrice) * h.shares : undefined
                  const plPct = h.avgPrice ? ((h.lastPrice || 0) - h.avgPrice) / h.avgPrice * 100 : undefined
                  return (
                    <div key={h.playerId} className="p-3 bg-zinc-950 border border-zinc-800 rounded flex items-center justify-between">
                      <div>
                        <div className="text-zinc-100 font-medium">{h.playerId}</div>
                        <div className="text-zinc-400 text-sm">{h.shares} shares @ {h.avgPrice != null ? <span className="font-mono">${h.avgPrice.toFixed(2)}</span> : '—'}</div>
                      </div>
                      <div className="text-right">
                        <div className="font-mono text-zinc-100">${(h.value || 0).toFixed(2)}</div>
                        <div className="text-sm text-zinc-400">{h.lastPrice != null ? <span className="font-mono">${h.lastPrice.toFixed(2)}</span> : 'Price N/A'}</div>
                        {pl != null && (
                          <div className={`text-sm mt-1 ${pl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>${pl.toFixed(2)} {plPct != null ? `(${plPct.toFixed(1)}%)` : ''}</div>
                        )}
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
