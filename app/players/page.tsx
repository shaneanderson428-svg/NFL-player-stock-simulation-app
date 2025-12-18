import fs from 'fs'
import path from 'path'
import Link from 'next/link'
import React from 'react'

export default async function PlayersPage() {
  const dir = path.join(process.cwd(), 'data', 'history')
  let files: string[] = []
  try {
    files = await fs.promises.readdir(dir)
  } catch (err) {
    files = []
  }

  const suffix = '_price_history.json'
  const playerIds = files
    .filter((f) => f.endsWith(suffix))
    .map((f) => f.slice(0, -suffix.length))
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))

  // Attempt to load player names from data/player_stock_summary.csv (preferred)
  const nameMap: Record<string, string> = {}
  try {
    const csvPath = path.join(process.cwd(), 'data', 'player_stock_summary.csv')
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

  return (
    <div style={{ padding: 20 }}>
      <h1 style={{ marginTop: 0 }}>Players</h1>
      <ul>
        {playerIds.map((id) => (
          <li key={id}>
            <Link href={`/player/${id}`}>
              {nameMap[id] ? `${nameMap[id]} (${id})` : id}
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}

export const dynamic = 'force-dynamic'
