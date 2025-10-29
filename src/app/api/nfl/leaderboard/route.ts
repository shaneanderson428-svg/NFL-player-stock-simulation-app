import fs from "fs";
import path from "path";
import { NextResponse } from "next/server";
import { parse as csvParseSync } from "csv-parse/sync";

type CacheEntry = {
  mtimeMs: number;
  rows: Array<Record<string, any>>;
};

const cache: Record<string, CacheEntry> = {};

function coerceValue(v: string) {
  if (v === null || v === undefined) return v;
  const t = v.trim();
  if (t === '') return null;
  // try integer
  if (/^-?\d+$/.test(t)) return parseInt(t, 10);
  // try float
  if (/^-?\d+\.\d+$/.test(t)) return parseFloat(t);
  // otherwise string
  return t;
}

export async function GET() {
  try {
    const filePath = path.join(process.cwd(), "data/epa_cpoe_summary_2025.csv");

    const st = fs.statSync(filePath);
    const mtimeMs = st.mtimeMs;

    const cached = cache[filePath];
    if (cached && cached.mtimeMs === mtimeMs) {
      return NextResponse.json({ ok: true, rows: cached.rows });
    }

    const raw = fs.readFileSync(filePath, "utf8");
    // use csv-parse to handle quoted fields and commas
    const records = csvParseSync(raw, {
      columns: true,
      skip_empty_lines: true,
    }) as Array<Record<string, string>>;

    // coerce values
    const rows = records.map((r) => {
      const out: Record<string, any> = {};
      Object.entries(r).forEach(([k, v]) => {
        out[k] = coerceValue(String(v));
      });
      return out;
    });

    // cache
    cache[filePath] = { mtimeMs, rows };

    return NextResponse.json({ ok: true, rows });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: err.message }, { status: 500 });
  }
}
