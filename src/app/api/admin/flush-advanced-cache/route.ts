import { NextResponse } from "next/server";
import fs from 'fs';
import path from 'path';

export async function POST() {
  try {
    const advancedPath = path.resolve(process.cwd(), 'data', 'advanced');
    if (fs.existsSync(advancedPath)) {
      const files = fs.readdirSync(advancedPath);
      for (const file of files) {
        if (file.endsWith('.json')) {
          const absolute = path.join(advancedPath, file);
          try {
            const resolved = require.resolve(absolute);
            if (require.cache && require.cache[resolved]) {
              delete require.cache[resolved];
            }
          } catch (e) {
            // ignore files that can't be resolved
          }
        }
      }
    }
    return NextResponse.json({ ok: true, message: 'Advanced cache flushed' });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: err?.message ?? String(err) }, { status: 500 });
  }
}
