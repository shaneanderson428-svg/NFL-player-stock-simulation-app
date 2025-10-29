const { spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');

function run(cmd, args, opts = {}) {
  console.log('> ', cmd, args.join(' '));
  const res = spawnSync(cmd, args, { stdio: 'inherit', ...opts });
  return res.status === 0;
}

function pythonExists() {
  try {
    const res = spawnSync('python3', ['--version']);
    return res.status === 0;
  } catch (e) {
    return false;
  }
}

async function main() {
  console.log('vercel_build: starting data generation (if python3 available)');
  if (!pythonExists()) {
    console.warn('vercel_build: python3 not found in build environment — skipping data generation');
    return;
  }

  const repoRoot = process.cwd();
  const scriptsDir = path.join(repoRoot, 'scripts');

  // compute player stock
  const computeScript = path.join(scriptsDir, 'compute_player_stock.py');
  if (fs.existsSync(computeScript)) {
    const ok = run('python3', [computeScript, '--input', 'data/player_game_stats.csv', '--output', 'data/player_stock_summary.csv']);
    if (!ok) console.warn('vercel_build: compute_player_stock.py failed (continuing build)');
  } else {
    console.warn('vercel_build: compute_player_stock.py not found — skipping');
  }

  // clean player profiles
  const cleanScript = path.join(scriptsDir, 'clean_player_profiles.py');
  if (fs.existsSync(cleanScript)) {
    const ok2 = run('python3', [cleanScript]);
    if (!ok2) console.warn('vercel_build: clean_player_profiles.py failed (continuing build)');
  } else {
    console.warn('vercel_build: clean_player_profiles.py not found — skipping');
  }

  console.log('vercel_build: data generation step complete');
}

main().catch(err => {
  console.error('vercel_build: unexpected error', err);
  process.exitCode = 1;
});
