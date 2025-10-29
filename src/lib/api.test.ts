import { describe, it, expect } from 'vitest';
import { mapNormalizedToAthlete, normalizePlayersFromEspnRoster } from './api';

describe('mapNormalizedToAthlete', () => {
  it('maps minimal player object to Athlete with defaults', () => {
    const raw = { id: 123, name: 'John Doe', team: { abbreviation: 'CHI' } };
    const a = mapNormalizedToAthlete(raw as any);
    expect(a.id).toBe('123');
    expect(a.name).toBe('John Doe');
    expect(a.team).toBe('CHI');
    expect(a.sport).toBe('Football');
    expect(typeof a.currentPrice).toBe('number');
  });
});

describe('normalizePlayersFromEspnRoster', () => {
  it('extracts players from a simple items array', () => {
    const payload = { items: [{ id: 1, fullName: 'A One' }, { id: 2, fullName: 'B Two' }] };
    const players = normalizePlayersFromEspnRoster(payload as any, 'CHI');
    expect(players.length).toBe(2);
    expect(players[0].name).toContain('A One');
  });
});
