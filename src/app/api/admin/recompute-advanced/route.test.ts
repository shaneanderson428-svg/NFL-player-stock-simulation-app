import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';

// Mock child_process.spawn before importing the route so it uses the mock.
const mockSpawn = vi.fn();
vi.mock('child_process', () => ({ spawn: (...args: any[]) => mockSpawn(...args) }));

import { POST } from './route';

describe('/api/admin/recompute-advanced route', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it('returns 401 when secret missing/invalid', async () => {
    process.env.ADVANCED_SECRET = 'supersecret';
    const req = new Request('http://localhost/api/admin/recompute-advanced', { method: 'POST' });
    const res = await POST(req as any);
    const json = await (res as any).json();
    expect(json.ok).toBe(false);
    expect(json.error).toBeTruthy();
  });

  it('spawns compute script when secret provided', async () => {
    process.env.ADVANCED_SECRET = 'supersecret';
    // mock spawn behavior: emit close immediately with exit code 0
    mockSpawn.mockImplementation(() => {
      const events: any = {};
      return {
        stdout: { on: () => {} },
        stderr: { on: () => {} },
        on: (ev: string, cb: any) => { if (ev === 'close') cb(0); }
      };
    });

    const req = new Request('http://localhost/api/admin/recompute-advanced', { method: 'POST', headers: { 'x-advanced-secret': 'supersecret' } });
    const res = await POST(req as any);
    const json = await (res as any).json();
    expect(json.ok).toBe(true);
    expect(typeof json.exitCode).toBe('number');
  });
});
