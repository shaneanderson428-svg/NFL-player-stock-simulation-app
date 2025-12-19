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

  if (!portfolio) return null

  const pf = portfolio as Portfolio
  const pos = pf.positions[playerId] ?? { shares: 0, avgPrice: 0 }
  const positionValue = price != null ? pos.shares * price : 0
  const portfolioValue = pf.cash + positionValue

  function updateAndSave(next: Portfolio) {
    setPortfolio(next)
    savePortfolio(next)
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
    setMessage(`Bought ${n} share${n !== 1 ? 's' : ''} @ $${price.toFixed(2)}`)
  }

  function sell(n: number) {
    if (price == null) {
      setMessage('Price unavailable')
      return
    }
    const existing = pf.positions[playerId] ?? { shares: 0, avgPrice: 0 }
    if (n > existing.shares) {
      setMessage('Not enough shares to sell')
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
    setMessage(`Sold ${n} share${n !== 1 ? 's' : ''} @ $${price.toFixed(2)}`)
  }

  return (
    <div className="mt-6 p-4 bg-zinc-900 border border-zinc-800 rounded-xl text-zinc-100">
      <h3 className="text-lg font-semibold mb-2">Portfolio</h3>
      <div className="flex items-center gap-6">
        <div>
          <div className="text-sm text-zinc-400">Cash</div>
          <div className="text-zinc-100 font-medium font-mono">${pf.cash.toFixed(2)}</div>
        </div>

        <div>
          <div className="text-sm text-zinc-400">Shares ({playerId})</div>
          <div className="text-zinc-100 font-medium">{pos.shares} @ {pos.avgPrice ? <span className="font-mono">${pos.avgPrice.toFixed(2)}</span> : '—'}</div>
        </div>

        <div>
          <div className="text-sm text-zinc-400">Position Value</div>
          <div className="text-zinc-100 font-medium">{price != null ? <span className="font-mono">${positionValue.toFixed(2)}</span> : '—'}</div>
        </div>

        <div>
          <div className="text-sm text-zinc-400">Portfolio (cash + this position)</div>
          <div className="text-zinc-100 font-medium">${portfolioValue.toFixed(2)}</div>
        </div>
      </div>

      <div className="mt-4 flex items-center gap-3">
        <input
          type="number"
          min={1}
          value={qty}
          onChange={(e) => setQty(Math.max(1, Number(e.target.value || 1)))}
          className="w-24 py-1 px-2 rounded-md bg-zinc-800 border border-zinc-700 text-zinc-100"
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
        {loadingPrice ? <div className="text-zinc-400">Loading price…</div> : price == null ? <div className="text-zinc-400">Price N/A</div> : <div className="text-zinc-400">Price: <span className="font-mono">${price.toFixed(2)}</span></div>}
      </div>

      {message ? <div className="mt-3 text-sm text-zinc-200">{message}</div> : null}
    </div>
  )
}
