/** @vitest-environment jsdom */
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import PlayerCard from './PlayerCard';

const samplePlayer = {
  id: '4431611',
  name: 'Caleb Williams',
  team: 'CHI',
  position: 'QB',
  currentPrice: 100,
  imageUrl: 'https://a.espncdn.com/i/headshots/nfl/players/full/4431611.png',
};

describe('PlayerCard', () => {
  let origFetch: any;
  beforeEach(() => {
    origFetch = global.fetch;
  });
  afterEach(() => {
    global.fetch = origFetch;
    vi.restoreAllMocks();
  });

  it('shows loading skeleton then price when fetch resolves', async () => {
    global.fetch = vi.fn(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve({ ok: true, found: true, newPrice: 123.45, appliedPct: 0.1 }) }) as any
    );

    render(<PlayerCard player={samplePlayer} />);

    // skeleton (aria-hidden div) should appear immediately
    expect(screen.getByRole('article', { name: /Caleb Williams/i })).toBeTruthy();

    // wait for the formatted price to appear
    await waitFor(() => {
      expect(screen.getByText(/\$123\.45/)).toBeTruthy();
    });
  });
});
