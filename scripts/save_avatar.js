#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

if (process.argv.length < 4) {
  console.error('Usage: node scripts/save_avatar.js <espnId> <path-to-image>');
  process.exit(2);
}
const espnId = process.argv[2];
const src = process.argv[3];

if (!fs.existsSync(src)) {
  console.error('Source image not found:', src);
  process.exit(3);
}

const ext = path.extname(src).toLowerCase() || '.jpg';
const destDir = path.join(process.cwd(), 'public', 'avatars');
if (!fs.existsSync(destDir)) fs.mkdirSync(destDir, { recursive: true });
const dest = path.join(destDir, `${espnId}${ext}`);
fs.copyFileSync(src, dest);
console.log('Wrote avatar to', dest);
