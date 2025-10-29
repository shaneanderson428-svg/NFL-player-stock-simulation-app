import fs from 'fs';
import path from 'path';
import { test, expect } from 'vitest';
import { GET as stocksGET } from './route';

test('stocks route returns parsed rows from CSV', async () => {
  const filePath = path.join(process.cwd(), 'data/player_stock_summary.csv');
  const sample = `player,latest_week,stock,confidence,epa_z,cpoe_z,pass_attempts
Alice,10,110.0,0.9,1.0,0.5,25
Bob,10,95.0,0.8,-0.5,-0.2,21
`;

  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, sample, 'utf8');

  try {
  const res = await stocksGET(new Request('http://localhost/api/nfl/stocks'));
    const body = await res.json();
    expect(body.ok).toBe(true);
    expect(Array.isArray(body.rows)).toBe(true);
    expect(body.rows.length).toBeGreaterThanOrEqual(2);
    expect(body.rows[0]).toHaveProperty('player');
    expect(body.rows[0]).toHaveProperty('stock');
  } finally {
    try { fs.unlinkSync(filePath); } catch (e) {}
  }
});

test('team enrichment does not overwrite existing team on row', async () => {
  const base = path.join(process.cwd(), 'data');
  const filePath = path.join(base, 'player_stock_summary.csv');
  const profilesPath = path.join(base, 'player_profiles_cleaned.csv');
  fs.mkdirSync(base, { recursive: true });
  const sample = `player,latest_week,stock,confidence,team,pass_attempts
Alice,10,110.0,0.9,OLD,25
`;
  fs.writeFileSync(filePath, sample, 'utf8');
  const profiles = `espnId,player,team,position
alice,Alice,NEW,QB
`;
  fs.writeFileSync(profilesPath, profiles, 'utf8');
  try {
  const res = await stocksGET(new Request('http://localhost/api/nfl/stocks'));
    const body = await res.json();
    expect(body.ok).toBe(true);
    const row = body.rows.find((r: any) => String(r.player) === 'Alice');
    expect(row).toBeTruthy();
    expect(row.team).toBe('OLD');
  } finally {
    try { fs.unlinkSync(filePath); } catch (e) {}
    try { fs.unlinkSync(profilesPath); } catch (e) {}
  }
});

test('history append preserves older entries (no overwrite)', async () => {
  const base = path.join(process.cwd(), 'data');
  const filePath = path.join(base, 'player_stock_summary.csv');
  const histPath = path.join(base, 'player_stock_history.csv');
  fs.mkdirSync(base, { recursive: true });
  const sample = `player,latest_week,stock,confidence,pass_attempts
Bob,10,95.0,0.8,22
`;
  fs.writeFileSync(filePath, sample, 'utf8');
  const history = `player,week,stock
Bob,1,90
Bob,2,95
`;
  fs.writeFileSync(histPath, history, 'utf8');
  try {
  const res = await stocksGET(new Request('http://localhost/api/nfl/stocks'));
    const body = await res.json();
    expect(body.ok).toBe(true);
    const row = body.rows.find((r: any) => String(r.player) === 'Bob');
    expect(row).toBeTruthy();
    expect(Array.isArray(row.history)).toBe(true);
    expect(row.history.length).toBeGreaterThanOrEqual(2);
    expect(Number(row.history[0].week)).toBeLessThan(Number(row.history[1].week));
  } finally {
    try { fs.unlinkSync(filePath); } catch (e) {}
    try { fs.unlinkSync(histPath); } catch (e) {}
  }
});

