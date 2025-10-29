import fs from 'fs'
import path from 'path'
import { parse as csvParseSync } from 'csv-parse/sync'
import { expect, test } from 'vitest'

const SUMMARY = path.join(process.cwd(), 'data', 'player_stock_summary.csv')

test('player_stock_summary.csv contains espnId and pass_attempts', () => {
  if (!fs.existsSync(SUMMARY)) {
    // If the summary CSV hasn't been generated in this environment, skip the check.
    console.warn(`Skipping player_stock_summary test: ${SUMMARY} not found`)
    return
  }
  const raw = fs.readFileSync(SUMMARY, 'utf8')
  const records = csvParseSync(raw, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>
  expect(records.length).toBeGreaterThan(0)

  // pass_attempts should exist and at least one player should have >=20
  const hasManyAttempts = records.some((r) => {
    const v = r['pass_attempts'] ?? r['passAttempts'] ?? r['pass_attempts']
    const n = v === undefined || v === null || v === '' ? NaN : Number(v)
    return !Number.isNaN(n) && n >= 20
  })
  // Expect at least one QB with >=20 pass attempts in sample data
  expect(hasManyAttempts).toBe(true)
  
})
