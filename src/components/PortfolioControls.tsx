"use client"
import React, { useEffect, useState } from 'react'

type Position = {
  shares: number
  avgPrice: number
}

type Portfolio = {
  cash: number
  positions: Record<string, Position>
}

const STORAGE_KEY = 'nfl_portfolio_v1'

function loadPortfolio(): Portfolio {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return { cash: 10000, positions: {} }
    const parsed = JSON.parse(raw)
    return {
      cash: typeof parsed.cash === 'number' ? parsed.cash : 10000,
      positions: parsed.positions ?? {},
    }
  } catch (err) {
    return { cash: 10000, positions: {} }
  }
}

function savePortfolio(p: Portfolio) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(p))
  } catch (err) {
    // ignore
  }
}

export default function PortfolioControls({ playerId }: { playerId: string }) {
  const [portfolio, setPortfolio] = useState<Portfolio | null>(null)
  const [price, setPrice] = useState<number | null>(null)
  const [qty, setQty] = useState<number>(1)
  const [loadingPrice, setLoadingPrice] = useState(true)
  const [message, setMessage] = useState<string | null>(null)

  useEffect(() => {
    // load portfolio from localStorage
    setPortfolio(loadPortfolio())
  }, [])

  useEffect(() => {
    let mounted = true
    setLoadingPrice(true)
    fetch(`/api/history/${playerId}`)
      .then((r) => r.json())
      .then((arr) => {
        if (!mounted) return
        if (Array.isArray(arr) && arr.length) {
          const last = Number(arr[arr.length - 1].price)
          setPrice(Number.isFinite(last) ? last : null)
        } else {
          setPrice(null)
        }
      })
      .catch(() => {
        if (!mounted) return
        setPrice(null)
      })
      .finally(() => mounted && setLoadingPrice(false))
    return () => {
      mounted = false
    }
  }, [playerId])

  // auto-clear messages after a short delay so UI stays clean
  useEffect(() => {
    if (!message) return
    const t = setTimeout(() => setMessage(null), 3000)
    return () => clearTimeout(t)
  }, [message])

  if (!portfolio) return null

  const pf = portfolio as Portfolio
  const pos = pf.positions[playerId] ?? { shares: 0, avgPrice: 0 }
  const positionValue = price != null ? pos.shares * price : 0
  const portfolioValue = pf.cash + positionValue

  function updateAndSave(next: Portfolio) {
    setPortfolio(next)
    savePortfolio(next)
    // notify other components in the same window so they update immediately
    try {
      if (typeof window !== 'undefined') {
        // debug: notify other components and log that we dispatched
        console.debug('[PortfolioControls] dispatch portfolio:changed', playerId, next)
        window.dispatchEvent(new CustomEvent('portfolio:changed', { detail: { playerId, portfolio: next } }))
      }
    } catch (err) {
      // ignore
    }
  }

  // simple market impact model: small price move per share bought/sold
  function applyMarketImpact(playerId: string, qty: number, isBuy: boolean) {
    if (price == null) return
    try {
      const KEY = 'nfl_market_overrides'
      const raw = localStorage.getItem(KEY)
      const current = raw ? JSON.parse(raw) : {}
      // delta percent per share (0.5% per share), capped
      const sign = isBuy ? 1 : -1
      const deltaPct = Math.min(0.2, 0.005 * qty) * sign
      const newPrice = +(price * (1 + deltaPct))
      current[playerId] = { price: +newPrice.toFixed(2), at: Date.now() }
      localStorage.setItem(KEY, JSON.stringify(current))
      console.debug('[PortfolioControls] market override set', playerId, current[playerId])
      window.dispatchEvent(new CustomEvent('market:changed', { detail: { playerId, price: current[playerId].price } }))
    } catch (err) {
      // ignore
    }
  }

  function buy(n: number) {
    if (price == null) {
      setMessage('Price unavailable')
      return
    }
    const cost = price * n
    if (cost > pf.cash) {
      setMessage('Insufficient cash')
      return
    }
    const existing = pf.positions[playerId] ?? { shares: 0, avgPrice: 0 }
    const newShares = existing.shares + n
    const newAvg = newShares === 0 ? 0 : (existing.shares * existing.avgPrice + n * price) / newShares
    const next: Portfolio = {
      cash: +(pf.cash - cost).toFixed(2),
      positions: { ...pf.positions, [playerId]: { shares: newShares, avgPrice: +newAvg.toFixed(4) } },
    }
    updateAndSave(next)
    setMessage(`Bought ${n}`)
    applyMarketImpact(playerId, n, true)
  }

  function sell(n: number) {
    if (price == null) {
      setMessage('Price unavailable')
      return
    }
    const existing = pf.positions[playerId] ?? { shares: 0, avgPrice: 0 }
    if (n > existing.shares) {
      setMessage('Not enough shares')
      return
    }
    const proceeds = price * n
    const newShares = existing.shares - n
    const nextPositions = { ...pf.positions }
    if (newShares <= 0) delete nextPositions[playerId]
    else nextPositions[playerId] = { shares: newShares, avgPrice: existing.avgPrice }

    const next: Portfolio = {
      cash: +(pf.cash + proceeds).toFixed(2),
      positions: nextPositions,
    }
    updateAndSave(next)
    setMessage(`Sold ${n}`)
    applyMarketImpact(playerId, n, false)
  }


  return (
    <div className="mt-4 p-4 bg-zinc-900 border border-zinc-800 rounded-xl text-zinc-100">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 items-end">
        <div>
          <div className="text-xs text-zinc-400">Cash</div>
          <div className="text-zinc-100 font-medium font-mono">${pf.cash.toFixed(2)}</div>
        </div>

        <div>
          <div className="text-xs text-zinc-400">Shares</div>
          <div className="text-zinc-100 font-medium font-mono">{Math.floor(pos.shares)}</div>
        </div>

        <div>
          <div className="text-xs text-zinc-400">Avg</div>
          <div className="text-zinc-100 font-medium">{pos.avgPrice ? <span className="font-mono">${pos.avgPrice.toFixed(2)}</span> : '—'}</div>
        </div>

        <div>
          <div className="text-xs text-zinc-400">Total</div>
          <div className="text-zinc-100 font-medium">${portfolioValue.toFixed(2)}</div>
        </div>
      </div>

      <div className="mt-3 flex items-center gap-3">
        <input
          type="number"
          min={1}
          value={qty}
          onChange={(e) => setQty(Math.max(1, Number(e.target.value || 1)))}
          className="w-20 py-1 px-2 rounded-md bg-zinc-800 border border-zinc-700 text-zinc-100"
        />
        <button
          className="bg-emerald-600 hover:bg-emerald-500 text-zinc-950 font-semibold py-2 px-4 rounded-md"
          onClick={() => buy(qty)}
        >
          Buy
        </button>
        <button
          className="bg-red-600 hover:bg-red-500 text-zinc-100 font-semibold py-2 px-4 rounded-md"
          onClick={() => sell(qty)}
        >
          Sell
        </button>

        <div className="ml-auto text-zinc-400">{loadingPrice ? 'Loading…' : price == null ? 'N/A' : <span className="font-mono">${price.toFixed(2)}</span>}</div>
      </div>

      {message ? <div className="mt-3 text-sm text-zinc-200">{message}</div> : null}
    </div>
  )
}
