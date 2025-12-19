"use client"
import React, { useEffect, useState } from 'react'

const STORAGE_KEY = 'nfl_portfolio_v1'

function readShares(playerId: string) {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return 0
    const parsed = JSON.parse(raw)
    const pos = parsed.positions?.[playerId]
    return pos?.shares ?? 0
  } catch (err) {
    return 0
  }
}

export default function OwnedSharesBadge({ playerId }: { playerId: string }) {
  const [shares, setShares] = useState<number>(() => (typeof window !== 'undefined' ? readShares(playerId) : 0))
  const [flash, setFlash] = useState(false)

  useEffect(() => {
    const onStorage = (e: StorageEvent) => {
      console.debug('[OwnedSharesBadge] storage event', e.key)
      if (e.key === STORAGE_KEY || e.key === null) {
        setShares(readShares(playerId))
        setFlash(true)
        setTimeout(() => setFlash(false), 500)
      }
    }
    const onCustom = (e: any) => {
      console.debug('[OwnedSharesBadge] portfolio:changed event', e && e.detail)
      try {
        const detail = e?.detail
        if (!detail) return
        if (String(detail.playerId) === String(playerId)) {
          setShares(detail.portfolio?.positions?.[playerId]?.shares ?? readShares(playerId))
          setFlash(true)
          setTimeout(() => setFlash(false), 500)
        }
      } catch (_) {}
    }
    window.addEventListener('storage', onStorage)
    window.addEventListener('portfolio:changed', onCustom as EventListener)
    return () => {
      window.removeEventListener('storage', onStorage)
      window.removeEventListener('portfolio:changed', onCustom as EventListener)
    }
  }, [playerId])

  if (!shares) return null

  return (
    <div className={`ml-3 inline-flex items-center px-2 py-1 rounded-full text-xs font-semibold ${flash ? 'ring-2 ring-emerald-500' : 'ring-0'} bg-zinc-800 text-zinc-100`}>
      {shares}x
    </div>
  )
}
