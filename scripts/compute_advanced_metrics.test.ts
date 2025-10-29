import { test, expect } from 'vitest';
import { spawnSync } from 'child_process';
import fs from 'fs';
import path from 'path';

const repoRoot = path.resolve(__dirname, '..');
const script = path.join(repoRoot, 'scripts', 'compute_advanced_metrics.py');

const pythonAvailable = (() => {
  try {
    const r = spawnSync('python3', ['--version']);
    return r.status === 0;
  } catch (e) {
    return false;
  }
})();

(pythonAvailable ? test : test.skip)('compute_advanced_metrics.py smoke run', () => {
  const outDir = path.join(repoRoot, 'data', 'advanced-test');
  if (fs.existsSync(outDir)) {
    fs.rmSync(outDir, { recursive: true, force: true });
  }

  const res = spawnSync('python3', [script, '--input', path.join(repoRoot, 'data', 'pbp'), '--output', outDir], { stdio: 'inherit' });
  expect(res.status).toBe(0);
  // index.json should be created
  const idx = path.join(outDir, 'index.json');
  expect(fs.existsSync(idx)).toBe(true);

  // clean up
  try { fs.rmSync(outDir, { recursive: true, force: true }); } catch (e) {}
});
