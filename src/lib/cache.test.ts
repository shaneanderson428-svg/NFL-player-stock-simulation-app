import { describe, it, expect, beforeEach } from 'vitest';
import { getIfFresh, getAny, setCached, clearCache } from './cache';

describe('cache helper', () => {
  beforeEach(() => clearCache());

  it('stores and retrieves fresh values', () => {
    setCached('k1', { v: 1 });
    const v = getIfFresh('k1', 1000);
    expect(v).not.toBeNull();
    expect((v as any).v).toBe(1);
  });

  it('returns null for stale values with getIfFresh but getAny returns it', async () => {
    setCached('k2', { v: 2 });
    // simulate time passing by reading any then using small TTL
    const any = getAny('k2');
    expect(any).not.toBeNull();
    const stale = getIfFresh('k2', -1);
    expect(stale).toBeNull();
  });
});
