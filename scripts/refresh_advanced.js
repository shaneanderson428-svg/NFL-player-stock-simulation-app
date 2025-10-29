#!/usr/bin/env node
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const http = require('http');

const root = path.resolve(__dirname, '..');
const advancedDir = path.join(root, 'data', 'advanced');
const regenScript = path.join(root, 'scripts', 'regenerate_advanced_index.js');
const computePy = path.join(root, 'scripts', 'compute_advanced_metrics.py');

function log(...args){ console.log('[refresh_advanced]', ...args); }

try{
  // 1) Run compute script if present
  if (fs.existsSync(computePy)){
    log('Running compute_advanced_metrics.py (this may take a while)...');
    execSync(`python3 "${computePy}"`, { stdio: 'inherit', cwd: root, env: process.env });
  } else {
    log('No compute script found, skipping compute step.');
  }

  // 2) Regenerate index
  if (fs.existsSync(regenScript)){
    log('Regenerating data/advanced/index.json...');
    execSync(`node "${regenScript}"`, { stdio: 'inherit', cwd: root, env: process.env });
  } else {
    log('No regenerate script found, skipping index regeneration.');
  }

  // 3) POST admin flush endpoint
  const flushUrl = process.env.FLUSH_URL || 'http://localhost:3000/api/admin/flush-advanced-cache';
  log('POSTing flush to', flushUrl);
  const flushReq = http.request(flushUrl, { method: 'POST' }, res => {
    let body = '';
    res.setEncoding('utf8');
    res.on('data', d=> body += d);
    res.on('end', ()=>{
      log('Flush response:', res.statusCode, body.slice(0,200));
      // 4) GET /players and save snapshot
      const playersUrl = process.env.PLAYERS_URL || 'http://localhost:3000/players';
      log('Fetching', playersUrl);
      http.get(playersUrl, r => {
        let h = '';
        r.setEncoding('utf8');
        r.on('data', c => h+=c);
        r.on('end', ()=>{
          const outDir = path.join(root, 'tmp');
          if (!fs.existsSync(outDir)) fs.mkdirSync(outDir);
          const outFile = path.join(outDir, 'players.html');
          fs.writeFileSync(outFile, h);
          log('Saved players snapshot to', outFile);
        });
      }).on('error', e => log('Failed to GET /players:', e.message));
    });
  });
  flushReq.on('error', e => log('Flush request failed:', e.message));
  flushReq.end();

}catch(err){
  console.error('[refresh_advanced] Error:', err && err.stack ? err.stack : err);
  process.exit(1);
}
