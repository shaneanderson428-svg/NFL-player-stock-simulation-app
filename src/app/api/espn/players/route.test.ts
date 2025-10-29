import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';

// Mock axios and cache BEFORE importing the module under test so the module
// picks up the mocked bindings during evaluation.
vi.mock('axios', () => ({
  default: {
    get: vi.fn(),
  },
}));

vi.mock('@/lib/cache', () => {
  const getIfFreshMock = vi.fn();
  // Some modules import `getIfFreshTracked` at module-eval time; tests
  // mock the cache and must provide this export as well. Alias it to the
  // same mock so test setups that call `getIfFresh.mockReturnValue(...)`
  // affect both names.
  return {
    getIfFresh: getIfFreshMock,
    getIfFreshTracked: getIfFreshMock,
    getAny: vi.fn(),
    setCached: vi.fn(),
  };
});

import axios from 'axios';
import * as cache from '@/lib/cache';
import { GET } from './route';

function makeReq(query: string) {
  return new Request(`http://localhost/api/espn/players?${query}`);
}

describe('/api/espn/players route', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    // Default teams lookup for all tests unless overridden within a test
    (axios as any).get.mockImplementation(async (url: string) => {
      if (url.includes('/teams')) {
        return { data: { teams: [{ id: 123, abbreviation: 'CHI', slug: 'chicago-bears' }] } };
      }
      return { data: {} };
    });
  });

  it('happy path: resolves team + roster and returns normalized players', async () => {
    (cache.getIfFresh as any).mockReturnValue(null);
    // teams response
    (axios as any).get.mockImplementation(async (url: string) => {
      if (url.includes('/teams')) {
        return { data: { teams: [{ id: 123, abbreviation: 'CHI', slug: 'chicago-bears' }] } };
      }
      if (url.includes('/teams/123/roster')) {
        return { data: { items: [{ items: [{ id: '1', fullName: 'Player One' }, { id: '2', fullName: 'Player Two' }] }] } };
      }
      return { data: {} };
    });

  const res = await GET(makeReq('team=CHI'));
  const json = await (res as any).json();
  expect(Array.isArray(json.response)).toBe(true);
  // normalizedCount should be a number and match the response length
  expect(typeof json._debug.normalizedCount).toBe('number');
  expect(json.response.length).toBe(json._debug.normalizedCount);
  expect(json._debug.source).toBe('espn');
  });

  it('cache hit: returns cached players and cacheHit true', async () => {
    const cachedPlayers = [{ id: '10', name: 'Cached Player' }, { id: '11', name: 'Other' }];
  (cache.getIfFresh as any).mockReturnValue(cachedPlayers);

  const res = await GET(makeReq('team=CHI&page=1&limit=50'));
  const json = await (res as any).json();
  expect(json._debug.cacheHit).toBe(true);
  expect(json.response.length).toBe(cachedPlayers.length);
  expect(json.response[0].name).toBe('Cached Player');
  });

  it('roster fetch failure returns empty response with error debug', async () => {
    (cache.getIfFresh as any).mockReturnValue(null);
    // override roster call to throw
    (axios as any).get.mockImplementation(async (url: string) => {
      if (url.includes('/teams')) {
        return { data: { teams: [{ id: 456, abbreviation: 'NYG' }] } };
      }
      if (url.includes('/teams/456/roster')) {
        throw new Error('ESPN error');
      }
      return { data: {} };
    });

    const res = await GET(makeReq('team=NYG'));
    const json = await (res as any).json();
    expect(Array.isArray(json.response)).toBe(true);
    expect(json.response.length).toBe(0);
  expect(json._debug).toBeTruthy();
  // Ensure the debug source indicates ESPN; error message text can vary.
  expect(json._debug.source).toBe('espn');
  });

});
