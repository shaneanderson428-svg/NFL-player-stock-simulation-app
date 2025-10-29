const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');

const filePath = path.join(process.cwd(), 'data/player_stock_summary.csv');
if (!fs.existsSync(filePath)) {
  console.error('file not found:', filePath);
  process.exit(2);
}
const raw = fs.readFileSync(filePath, 'utf8');
const records = parse(raw, { columns: true, skip_empty_lines: true });
console.log('parsed rows:', records.length);
console.log(records.slice(0,5));
