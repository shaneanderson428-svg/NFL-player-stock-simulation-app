import fs from 'fs';
import path from 'path';
import { parse as csvParseSync } from 'csv-parse/sync';

export async function readCSV(relPath: string) {
  const filePath = path.join(process.cwd(), relPath);
  if (!fs.existsSync(filePath)) return [];
  const raw = fs.readFileSync(filePath, 'utf8');
  const records = csvParseSync(raw, { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
  // Coerce simple numeric strings to numbers where obvious
  return records.map((r) => {
    const out: Record<string, any> = {};
    Object.entries(r).forEach(([k, v]) => {
      if (v === null || v === undefined) {
        out[k] = v;
        return;
      }
      const s = String(v).trim();
      if (s === '') {
        out[k] = null;
      } else if (/^-?\d+$/.test(s)) {
        out[k] = parseInt(s, 10);
      } else if (/^-?\d+\.\d+$/.test(s)) {
        out[k] = parseFloat(s);
      } else {
        out[k] = s;
      }
    });
    return out;
  });
}

export default readCSV;
