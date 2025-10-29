import { execSync } from 'child_process'
import fs from 'fs'
import path from 'path'
import { expect, test } from 'vitest'

const ROOT = path.resolve(__dirname, '../../')
const PBPDIR = path.join(ROOT, 'data', 'pbp')
const OUTDIR = path.join(ROOT, 'data', 'advanced_test_out')

// Smoke test: run compute_advanced_metrics.py pointing at demo pbp files and an isolated output dir
test('compute_advanced_metrics produces json files and index', () => {
  // cleanup
  if (fs.existsSync(OUTDIR)) {
    fs.rmSync(OUTDIR, { recursive: true, force: true })
  }
  fs.mkdirSync(OUTDIR, { recursive: true })

  const cmd = `python3 scripts/compute_advanced_metrics.py --input ${PBPDIR} --output ${OUTDIR}`
  // If there are no pbp files (e.g. demos were removed), skip this smoke test.
  const pbpFiles = fs.existsSync(PBPDIR) ? fs.readdirSync(PBPDIR).filter(Boolean) : []
  if (pbpFiles.length === 0) {
    console.log('No pbp files found; skipping compute smoke test')
    return
  }

  const out = execSync(cmd, { cwd: ROOT, env: process.env, stdio: 'pipe' }).toString()
  console.log(out)

  // index.json must exist
  const idx = path.join(OUTDIR, 'index.json')
  expect(fs.existsSync(idx)).toBe(true)
  const idxData = JSON.parse(fs.readFileSync(idx, 'utf8'))
  expect(idxData).toHaveProperty('players')
  expect(Array.isArray(idxData.players)).toBe(true)
  expect(idxData.players.length).toBeGreaterThan(0)

  // ensure each referenced file exists
  for (const p of idxData.players) {
    const filePath = path.join(OUTDIR, p.file)
    expect(fs.existsSync(filePath)).toBe(true)
    const j = JSON.parse(fs.readFileSync(filePath, 'utf8'))
    expect(j).toHaveProperty('espnId')
    expect(j).toHaveProperty('plays')
  }

  // cleanup after test
  fs.rmSync(OUTDIR, { recursive: true, force: true })
})
