"use client"

import React, { useEffect, useRef, useState } from 'react'

type PricePoint = { week: number; price: number }

export default function PlayerStockChart({ playerId }: { playerId: string }) {
  const [data, setData] = useState<PricePoint[] | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [size, setSize] = useState({ width: 600, height: 240 })
  const [hover, setHover] = useState<{ x: number; y: number; point?: PricePoint } | null>(null)

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

  const points = (data || []).map((d) => ({ x: d.week, y: d.price }))

  const xScale = (week: number) => {
    if (!points.length) return 0
    const xs = points.map((p) => p.x)
    const min = Math.min(...xs)
    const max = Math.max(...xs)
    if (min === max) return innerWidth / 2
    return ((week - min) / (max - min)) * innerWidth
  }

  const yScale = (price: number) => {
    if (!points.length) return innerHeight / 2
    const ys = points.map((p) => p.y)
    const min = Math.min(...ys)
    const max = Math.max(...ys)
    if (min === max) return innerHeight / 2
    // invert y: higher price -> lower pixel
    return innerHeight - ((price - min) / (max - min)) * innerHeight
  }

  // create a smooth SVG path using Catmull-Rom to Bezier
  function pathFromPoints(pts: { x: number; y: number }[]) {
    if (!pts.length) return ''
    if (pts.length === 1) return `M ${pts[0].x} ${pts[0].y}`
    const p = pts
    let d = `M ${p[0].x} ${p[0].y}`
    for (let i = 0; i < p.length - 1; i++) {
      const p0 = p[i - 1] || p[i]
      const p1 = p[i]
      const p2 = p[i + 1]
      const p3 = p[i + 2] || p[i + 1]
      const control1x = p1.x + (p2.x - p0.x) / 6
      const control1y = p1.y + (p2.y - p0.y) / 6
      const control2x = p2.x - (p3.x - p1.x) / 6
      const control2y = p2.y - (p3.y - p1.y) / 6
      d += ` C ${control1x} ${control1y}, ${control2x} ${control2y}, ${p2.x} ${p2.y}`
    }
    return d
  }

  const svgPoints = points.map((pt) => ({ x: xScale(pt.x), y: yScale(pt.y) }))

  function handleMouseMove(e: React.MouseEvent) {
    if (!containerRef.current) return
    const rect = containerRef.current.getBoundingClientRect()
    const x = e.clientX - rect.left - margin.left
    const y = e.clientY - rect.top - margin.top
    if (x < 0 || x > innerWidth || y < 0 || y > innerHeight) {
      setHover(null)
      return
    }
    // find nearest point
    let nearestIdx = -1
    let nearestDist = Infinity
    svgPoints.forEach((p, idx) => {
      const dx = p.x - x
      const dy = p.y - y
      const dist = Math.hypot(dx, dy)
      if (dist < nearestDist) {
        nearestDist = dist
        nearestIdx = idx
      }
    })
    if (nearestIdx >= 0) {
      const p = (data || [])[nearestIdx]
      setHover({ x: svgPoints[nearestIdx].x + margin.left, y: svgPoints[nearestIdx].y + margin.top, point: p })
    }
  }

  function handleMouseLeave() {
    setHover(null)
  }

  return (
    <div ref={containerRef} style={{ width: '100%', height: 280, position: 'relative', fontFamily: 'Inter, system-ui, sans-serif' }}>
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
          <defs>
            <linearGradient id={`grad-${playerId}`} x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="#0ea5e9" stopOpacity="0.6" />
              <stop offset="100%" stopColor="#0ea5e9" stopOpacity="0.05" />
            </linearGradient>
          </defs>
          <g transform={`translate(${margin.left},${margin.top})`}>
            {/* axes */}
            <line x1={0} y1={innerHeight} x2={innerWidth} y2={innerHeight} stroke="#e6e6e6" />
            <line x1={0} y1={0} x2={0} y2={innerHeight} stroke="#e6e6e6" />

            {/* area under curve */}
            <path d={`${pathFromPoints([{ x: 0, y: innerHeight }, ...svgPoints, { x: innerWidth, y: innerHeight }])}`} fill={`url(#grad-${playerId})`} stroke="none" />

            {/* line */}
            <path d={pathFromPoints(svgPoints)} fill="none" stroke="#0369a1" strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />

            {/* points */}
            {svgPoints.map((p, idx) => (
              <circle key={idx} cx={p.x} cy={p.y} r={3} fill="#0369a1" />
            ))}

            {/* y-axis labels: min/max */}
            {data && data.length > 0 && (() => {
              const ys = data.map(d => d.price)
              const min = Math.min(...ys)
              const max = Math.max(...ys)
              return (
                <g>
                  <text x={-8} y={0} textAnchor="end" fontSize={11} fill="#444">{max.toFixed(0)}</text>
                  <text x={-8} y={innerHeight} textAnchor="end" fontSize={11} fill="#444">{min.toFixed(0)}</text>
                </g>
              )
            })()}

            {/* x-axis labels: first and last week */}
            {data && data.length > 0 && (() => {
              const weeks = data.map(d => d.week)
              const first = weeks[0]
              const last = weeks[weeks.length - 1]
              return (
                <g>
                  <text x={0} y={innerHeight + 16} textAnchor="start" fontSize={11} fill="#444">W{first}</text>
                  <text x={innerWidth} y={innerHeight + 16} textAnchor="end" fontSize={11} fill="#444">W{last}</text>
                </g>
              )
            })()}
          </g>
        </svg>
      )}

      {/* tooltip */}
      {hover && hover.point && (
        <div style={{ position: 'absolute', left: hover.x + 8, top: hover.y - 12, background: 'rgba(17,24,39,0.95)', color: 'white', padding: '6px 8px', borderRadius: 6, fontSize: 12, pointerEvents: 'none' }}>
          <div style={{ fontWeight: 600 }}>W{hover.point.week}</div>
          <div>${hover.point.price.toFixed(2)}</div>
        </div>
      )}
    </div>
  )
}
