"use client"

import React, { useEffect, useRef, useState } from 'react'

type PricePoint = { week: number; price: number }
type Candle = { week: number; open: number; close: number; high: number; low: number }

export default function PlayerStockChart({ playerId }: { playerId: string }) {
  const [data, setData] = useState<PricePoint[] | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [size, setSize] = useState({ width: 600, height: 240 })
  const [hover, setHover] = useState<{ x: number; y: number; point?: Candle } | null>(null)

  useEffect(() => {
    let mounted = true
    setLoading(true)
    setError(null)
    setData(null)
    fetch(`/api/history/${encodeURIComponent(playerId)}`)
      .then(async (res) => {
        if (!res.ok) {
          const j = await res.json().catch(() => null)
          throw new Error(j?.message || `HTTP ${res.status}`)
        }
        return res.json()
      })
      .then((json: PricePoint[]) => {
        if (!mounted) return
        // sort by week
        const sorted = (json || []).slice().sort((a, b) => (a.week || 0) - (b.week || 0))
        setData(sorted)
      })
      .catch((err: any) => {
        if (!mounted) return
        setError(String(err?.message || err))
      })
      .finally(() => {
        if (!mounted) return
        setLoading(false)
      })
    return () => {
      mounted = false
    }
  }, [playerId])

  // resize observer
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const ro = new ResizeObserver(() => {
      const rect = el.getBoundingClientRect()
      setSize({ width: Math.max(200, Math.round(rect.width)), height: Math.max(120, Math.round(rect.height || 240)) })
    })
    ro.observe(el)
    // initialize
    const rect = el.getBoundingClientRect()
    setSize({ width: Math.max(200, Math.round(rect.width)), height: Math.max(120, Math.round(rect.height || 240)) })
    return () => ro.disconnect()
  }, [])

  // helpers
  const margin = { top: 12, right: 12, bottom: 24, left: 40 }
  const innerWidth = Math.max(0, size.width - margin.left - margin.right)
  const innerHeight = Math.max(0, size.height - margin.top - margin.bottom)

  // Build candlestick data from price points
  const sorted = (data || []).slice().sort((a, b) => (a.week || 0) - (b.week || 0))
  const candles: Candle[] = sorted.map((d, i) => {
    const close = d.price
    const open = i > 0 ? sorted[i - 1].price : d.price
    const high = Math.max(open, close) * 1.05
    const low = Math.min(open, close) * 0.95
    return { week: d.week, open, close, high, low }
  })

  const n = candles.length
  const step = n > 1 ? innerWidth / (n - 1) : innerWidth / 2
  const candleWidth = Math.max(6, Math.min(24, step * 0.5))

  const ys = candles.flatMap((c) => [c.low, c.high])
  const minY = ys.length ? Math.min(...ys) : 0
  const maxY = ys.length ? Math.max(...ys) : 1

  const xForIndex = (i: number) => {
    if (n === 1) return innerWidth / 2
    return i * step
  }

  const yScale = (price: number) => {
    if (minY === maxY) return innerHeight / 2
    return innerHeight - ((price - minY) / (maxY - minY)) * innerHeight
  }

  function handleMouseMove(e: React.MouseEvent) {
    if (!containerRef.current) return
    const rect = containerRef.current.getBoundingClientRect()
    const x = e.clientX - rect.left - margin.left
    const y = e.clientY - rect.top - margin.top
    if (x < 0 || x > innerWidth || y < 0 || y > innerHeight) {
      setHover(null)
      return
    }
    // find nearest candle by x
    let nearestIdx = 0
    let nearestDist = Infinity
    for (let i = 0; i < n; i++) {
      const cx = xForIndex(i)
      const dist = Math.abs(cx - x)
      if (dist < nearestDist) {
        nearestDist = dist
        nearestIdx = i
      }
    }
    const c = candles[nearestIdx]
    if (c) {
      const cx = xForIndex(nearestIdx)
      const cy = yScale(c.close)
      setHover({ x: cx + margin.left, y: cy + margin.top, point: { week: c.week, open: c.open, close: c.close, high: c.high, low: c.low } })
    }
  }

  function handleMouseLeave() {
    setHover(null)
  }

  return (
    <div ref={containerRef} className="w-full h-72 relative" style={{ fontFamily: 'Inter, system-ui, sans-serif' }}>
      {loading && (
        <div style={{ padding: 16 }}>Loading price history…</div>
      )}
      {error && (
        <div style={{ padding: 16, color: 'crimson' }}>Error loading history: {error}</div>
      )}
      {!loading && !error && (!data || data.length === 0) && (
        <div style={{ padding: 16 }}>No price history available.</div>
      )}

      {!loading && !error && data && data.length > 0 && (
  <svg width={size.width} height={size.height} onMouseMove={handleMouseMove} onMouseLeave={handleMouseLeave} style={{ display: 'block' }}>
          <g transform={`translate(${margin.left},${margin.top})`}>
            {/* dark background */}
            <rect x={0} y={0} width={innerWidth} height={innerHeight} fill="#071126" rx={6} />
            {/* axes lines */}
            <line x1={0} y1={innerHeight} x2={innerWidth} y2={innerHeight} stroke="#0f1724" />
            <line x1={0} y1={0} x2={0} y2={innerHeight} stroke="#0f1724" />

            {/* candlesticks */}
        {candles.map((c, i) => {
              const cx = xForIndex(i)
              const cyHigh = yScale(c.high)
              const cyLow = yScale(c.low)
              const cyOpen = yScale(c.open)
              const cyClose = yScale(c.close)
              const isUp = c.close >= c.open
              const bodyTop = Math.min(cyOpen, cyClose)
              const bodyHeight = Math.max(1, Math.abs(cyClose - cyOpen))
              const color = isUp ? '#16a34a' : '#dc2626'
              return (
                <g key={i}>
                  <line x1={cx} x2={cx} y1={cyHigh} y2={cyLow} stroke={color} strokeWidth={2} strokeLinecap="round" />
                  <rect x={cx - candleWidth / 2} y={bodyTop} width={candleWidth} height={bodyHeight} fill={color} rx={2} />
                </g>
              )
            })}

            {/* y-axis labels */}
            {candles.length > 0 && (() => {
              return (
                <g>
                  <text x={-8} y={0} textAnchor="end" fontSize={11} fill="#9ca3af">{maxY.toFixed(0)}</text>
                  <text x={-8} y={innerHeight} textAnchor="end" fontSize={11} fill="#9ca3af">{minY.toFixed(0)}</text>
                </g>
              )
            })()}

            {/* x-axis labels */}
            {candles.length > 0 && (() => {
              const first = candles[0].week
              const last = candles[candles.length - 1].week
              return (
                <g>
                  <text x={0} y={innerHeight + 16} textAnchor="start" fontSize={11} fill="#9ca3af">W{first}</text>
                  <text x={innerWidth} y={innerHeight + 16} textAnchor="end" fontSize={11} fill="#9ca3af">W{last}</text>
                </g>
              )
            })()}
          </g>
        </svg>
      )}

      {/* tooltip */}
      {hover && hover.point && (
        <div style={{ position: 'absolute', left: Math.min(size.width - 220, hover.x + 8), top: Math.max(8, hover.y - 44), width: 208, padding: '8px 10px', borderRadius: 8, fontSize: 13, pointerEvents: 'none' }} className="bg-zinc-900 text-zinc-100 border border-zinc-800 shadow-lg">
          <div className="flex items-baseline justify-between">
            <div className="font-medium">W{hover.point.week}</div>
            <div className="text-sm text-zinc-400">{hover.point.close >= hover.point.open ? <span className="text-emerald-400">▲</span> : <span className="text-red-400">▼</span>}</div>
          </div>
          <div className="mt-1 grid grid-cols-2 gap-2 text-zinc-300 text-sm">
            <div className="text-zinc-400">Open</div>
            <div className="font-mono text-zinc-100">${hover.point.open.toFixed(2)}</div>
            <div className="text-zinc-400">High</div>
            <div className="font-mono text-zinc-100">${hover.point.high.toFixed(2)}</div>
            <div className="text-zinc-400">Low</div>
            <div className="font-mono text-zinc-100">${hover.point.low.toFixed(2)}</div>
            <div className="text-zinc-400">Close</div>
            <div className="font-mono text-zinc-100">${hover.point.close.toFixed(2)}</div>
          </div>
        </div>
      )}
    </div>
  )
}
