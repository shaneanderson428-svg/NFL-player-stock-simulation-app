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
  
    // Ensure data/history is available in public for runtime/static serving on Vercel
    try {
      const srcDir = path.join(repoRoot, 'data', 'history');
      const destDir = path.join(repoRoot, 'public', 'history');
      if (fs.existsSync(srcDir)) {
        if (!fs.existsSync(destDir)) fs.mkdirSync(destDir, { recursive: true });
        // Recursive copy to handle nested dirs (e.g., per-season folders)
        function copyRecursive(src, dest) {
          const lst = fs.lstatSync(src);
          if (lst.isSymbolicLink()) {
            // skip symlinks in history directory
            return;
          }
          if (lst.isDirectory()) {
            if (!fs.existsSync(dest)) fs.mkdirSync(dest, { recursive: true });
            const children = fs.readdirSync(src);
            children.forEach((c) => copyRecursive(path.join(src, c), path.join(dest, c)));
          } else if (lst.isFile()) {
            fs.copyFileSync(src, dest);
          } else {
            // skip sockets, pipes, and other special files
            return;
          }
        }

        const items = fs.readdirSync(srcDir);
        let copied = 0;
        items.forEach((f) => {
          const s = path.join(srcDir, f);
          const d = path.join(destDir, f);
          try {
            copyRecursive(s, d);
            copied++;
          } catch (e) {
            console.warn('vercel_build: failed to copy', s, '->', d, e && e.message);
          }
        });
        console.log('vercel_build: copied data/history -> public/history (', copied, 'items)');
      } else {
        console.log('vercel_build: no data/history dir found to copy');
      }
    } catch (err) {
      console.warn('vercel_build: error copying history to public', err && err.message);
    }
}

main().catch(err => {
  console.error('vercel_build: unexpected error', err);
  process.exitCode = 1;
});
