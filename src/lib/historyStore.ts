import fs from 'fs/promises';
import path from 'path';

type PricePoint = { t: string; p: number };

const DATA_DIR = path.join(process.cwd(), 'data');
const FILE_PATH = path.join(DATA_DIR, 'price_history.json');
const SQLITE_PATH = path.join(DATA_DIR, 'price_history.db');

async function ensureDataDir() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
  } catch (e) {
    // ignore
  }
}

// Strategy: if REDIS_URL present, use Redis. Else try SQLite (better-sqlite3).
// If neither available or initialization fails, fall back to file-backed JSON.

let redisClient: any = null;
let sqliteDb: any = null;
let using = 'none';

async function tryInitRedis() {
  const url = process.env.REDIS_URL;
  if (!url) return false;
  try {
    // use runtime require via eval to avoid static analysis by the bundler
    const _req: any = (globalThis as any).require ?? eval('require');
  const IORedis = _req(['i','oredis'].join(''));
    redisClient = new IORedis(url);
    // basic ping
    await redisClient.ping();
    using = 'redis';
    return true;
  } catch (e) {
    redisClient = null;
    return false;
  }
}

async function tryInitSqlite() {
  try {
    // use runtime require via eval to avoid static analysis by the bundler
    const _req: any = (globalThis as any).require ?? eval('require');
  const Database = _req(['better','-','sqlite3'].join(''));
    await ensureDataDir();
    sqliteDb = new Database(SQLITE_PATH);
    sqliteDb.prepare('CREATE TABLE IF NOT EXISTS history (key TEXT PRIMARY KEY, json TEXT)').run();
    using = 'sqlite';
    return true;
  } catch (e) {
    sqliteDb = null;
    return false;
  }
}

async function initBackends() {
  if (using !== 'none') return;
  if (await tryInitRedis()) return;
  if (await tryInitSqlite()) return;
  // fallback to file
  using = 'file';
}

// Ensure we hydrate persistent backends from the canonical JSON file once per process.
let _hydrated = false;
async function hydrateFromFileOnce() {
  if (_hydrated) return;
  _hydrated = true;
  try {
    // Only hydrate if the canonical file exists
    let raw: string | null = null;
    try {
      raw = await fs.readFile(FILE_PATH, 'utf8');
    } catch (e) {
      // nothing to hydrate
      return;
    }
    if (!raw) return;
    const obj = JSON.parse(raw || '{}');
    const entries = Object.entries(obj || {});
    if (entries.length === 0) return;

    // ensure backends are initialized so we can write into them
    await initBackends();

    let players = 0;
    let points = 0;

    if (using === 'redis' && redisClient) {
      const pipeline = redisClient.pipeline();
      for (const [k, arr] of entries) {
        const key = String(k);
        pipeline.set(`hist:${key}`, JSON.stringify(arr));
        players += 1;
        points += Array.isArray(arr) ? arr.length : 0;
      }
      await pipeline.exec();
    } else if (using === 'sqlite' && sqliteDb) {
      const insert = sqliteDb.prepare('INSERT OR REPLACE INTO history (key, json) VALUES (?, ?)');
      const txn = sqliteDb.transaction((pairs: Array<[string, any]>) => {
        for (const [k, v] of pairs) insert.run(k, JSON.stringify(v));
      });
      txn(entries.map(([k, v]) => [String(k), v]));
      for (const [, arr] of entries) {
        players += 1;
        points += Array.isArray(arr) ? arr.length : 0;
      }
    } else {
      // file backend: nothing to do because FILE_PATH is already the canonical source
      for (const [, arr] of entries) {
        players += 1;
        points += Array.isArray(arr) ? arr.length : 0;
      }
    }

    // Log hydration summary so developers know persisted history was loaded
    try {
      // eslint-disable-next-line no-console
      console.log('[history] hydrated', players, 'players with', points, 'points from', FILE_PATH);
    } catch (e) {
      // ignore logging errors
    }
  } catch (e) {
    // ignore hydrate errors
  }
}

// Debounced write queue for SQLite/file backends
const pendingWrites = new Map<string, PricePoint[]>();
let flushTimer: NodeJS.Timeout | null = null;
const FLUSH_DEBOUNCE_MS = Number(process.env.HISTORY_FLUSH_MS ?? 2000);

function scheduleFlush() {
  if (flushTimer) return;
  flushTimer = setTimeout(async () => {
    try {
      const snapshot = new Map(pendingWrites);
      pendingWrites.clear();
      flushTimer = null;
      await initBackends();
      if (using === 'sqlite' && sqliteDb) {
        const insert = sqliteDb.prepare('INSERT OR REPLACE INTO history (key, json) VALUES (?, ?)');
        const txn = sqliteDb.transaction((entries: Array<[string, any]>) => {
          for (const [k, v] of entries) insert.run(k, JSON.stringify(v));
        });
        txn(Array.from(snapshot.entries()));
        return;
      }

      if (using === 'file') {
        await ensureDataDir();
        let obj: Record<string, PricePoint[]> = {};
        try {
          const raw = await fs.readFile(FILE_PATH, 'utf8');
          obj = JSON.parse(raw || '{}');
        } catch (e) {
          obj = {};
        }
        for (const [k, v] of snapshot.entries()) {
          obj[k] = v;
        }
        await fs.writeFile(FILE_PATH, JSON.stringify(obj, null, 2), 'utf8');
      }
    } catch (e) {
      // ignore
      flushTimer = null;
    }
  }, FLUSH_DEBOUNCE_MS);
}

export async function loadMap(): Promise<Map<string, PricePoint[]>> {
  await initBackends();
  // Hydrate persistent backend from canonical JSON once per process so
  // servers that use Redis/SQLite will see the file-backed history.
  try {
    await hydrateFromFileOnce();
  } catch (e) {
    // ignore
  }
  const m = new Map<string, PricePoint[]>();
  try {
    if (using === 'redis' && redisClient) {
      const keys = await redisClient.keys('hist:*');
      for (const k of keys) {
        const raw = await redisClient.get(k);
        try {
          const arr = JSON.parse(raw || '[]');
          m.set(k.replace(/^hist:/, ''), arr.map((it: any) => ({ t: String(it.t), p: Number(it.p) })));
        } catch (e) {
          // ignore
        }
      }
      return m;
    }

    if (using === 'sqlite' && sqliteDb) {
      const rows = sqliteDb.prepare('SELECT key, json FROM history').all();
      for (const r of rows) {
        try {
          const arr = JSON.parse(r.json || '[]');
          m.set(r.key, arr.map((it: any) => ({ t: String(it.t), p: Number(it.p) })));
        } catch (e) {
          // ignore
        }
      }
      return m;
    }

    // file fallback
    try {
      const raw = await fs.readFile(FILE_PATH, 'utf8');
      const obj = JSON.parse(raw || '{}');
      for (const k of Object.keys(obj)) {
        const arr = obj[k];
        if (Array.isArray(arr)) {
          m.set(k, arr.map((it: any) => ({ t: String(it.t), p: Number(it.p) })));
        }
      }
    } catch (e) {
      // empty
    }
    return m;
  } catch (e) {
    return m;
  }
}

export async function loadInto(target: Map<string, PricePoint[]>) {
  const m = await loadMap();
  for (const [k, v] of m.entries()) {
    if (!target.has(k)) target.set(k, v);
  }
}

export async function saveMap(source: Map<string, PricePoint[]>) {
  await initBackends();
  try {
    if (using === 'redis' && redisClient) {
      const pipeline = redisClient.pipeline();
      for (const [k, v] of source.entries()) {
        pipeline.set(`hist:${k}`, JSON.stringify(v));
      }
      await pipeline.exec();
      return;
    }

    if (using === 'sqlite' && sqliteDb) {
      const insert = sqliteDb.prepare('INSERT OR REPLACE INTO history (key, json) VALUES (?, ?)');
      const txn = sqliteDb.transaction((entries: Array<[string, any]>) => {
        for (const [k, v] of entries) insert.run(k, JSON.stringify(v));
      });
      txn(Array.from(source.entries()));
      return;
    }

    // file fallback: write full file
    await ensureDataDir();
    const obj: Record<string, PricePoint[]> = {};
    for (const [k, v] of source.entries()) obj[k] = v;
    await fs.writeFile(FILE_PATH, JSON.stringify(obj, null, 2), 'utf8');
  } catch (e) {
    // ignore
  }
}

export async function appendPoint(key: string, point: PricePoint, maxPoints = 240) {
  await initBackends();
  try {
    if (using === 'redis' && redisClient) {
      // use list for efficient append/trim and keep Redis immediate
      const listKey = `hist:${key}`;
      await redisClient.rpush(listKey, JSON.stringify(point));
      await redisClient.ltrim(listKey, -maxPoints, -1);
      return;
    }

    // For sqlite/file backends, update an in-memory pending buffer and flush debounced
    // Merge with any existing pending writes
    let current: PricePoint[] = pendingWrites.get(key) || [];
    if (current.length === 0) {
      // try to load existing on-disk value to merge
      try {
        if (using === 'sqlite' && sqliteDb) {
          const row = sqliteDb.prepare('SELECT json FROM history WHERE key = ?').get(key);
          if (row && row.json) current = JSON.parse(row.json || '[]');
        } else {
          const raw = await fs.readFile(FILE_PATH, 'utf8');
          const obj = JSON.parse(raw || '{}');
          current = obj[key] || [];
        }
      } catch (e) {
        current = [];
      }
    }

    const last = current[current.length - 1];
    const today = point.t.slice(0, 10);
    if (last && last.t && last.t.slice(0, 10) === today) {
      last.t = point.t;
      last.p = point.p;
    } else {
      current.push(point);
      if (current.length > maxPoints) current.shift();
    }

    pendingWrites.set(key, current);
    scheduleFlush();
    return;
  } catch (e) {
    // ignore
  }
}

export default { loadMap, loadInto, saveMap, appendPoint };
