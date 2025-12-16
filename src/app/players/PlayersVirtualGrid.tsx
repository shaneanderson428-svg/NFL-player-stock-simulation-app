"use client";
import React, { useRef, useState, useEffect, useCallback } from 'react';
import { FixedSizeList as List, ListOnScrollProps } from 'react-window';
import PlayerCard from '@/app/components/PlayerCard';

type PlayerShape = {
  espnId: string;
  name?: string;
  position?: string;
  team?: string;
  priceHistory?: any[];
  stock?: number;
  confidence?: number;
  history?: any[];
  _raw?: any;
};

type Props = {
  players?: PlayerShape[];
  // optional row height override
  rowHeight?: number;
};

export default function PlayersVirtualGrid({ players: initialPlayers, rowHeight = 180 }: Props) {
  const listRef = useRef<any | null>(null);
  const [players, setPlayers] = React.useState<PlayerShape[]>(initialPlayers ?? []);
  const [loading, setLoading] = React.useState<boolean>(!initialPlayers);

  // If no players prop is provided, fetch the roster client-side to avoid
  // large hydration payloads and potential client/server mismatch errors.
  useEffect(() => {
    if (initialPlayers && initialPlayers.length) return;
    let mounted = true;
    setLoading(true);
    (async () => {
      try {
        // Respect any querystring filters (e.g. ?all=1&position=WR) when the
        // client fetches the full roster. Use the current window.location.search
        // so links like `/players?all=1&position=WR` return only WRs.
        const search = (typeof window !== 'undefined' && window.location.search) ? window.location.search : '';
        const params = new URLSearchParams(search);
        // ensure all=1 is present for the full roster fetch
        if (!params.has('all')) params.set('all', '1');
        const q = params.toString();
        const res = await fetch(`/api/nfl/stocks?${q}`);
        const json = await res.json();
        const list = Array.isArray(json?.players) ? json.players : [];
        const shaped = list.map((p: any) => ({
          espnId: String(p.espnId || p.id || ''),
          name: p.name || p.player || '',
          position: p.position || p.position_profile || '',
          team: p.team || '',
          priceHistory: Array.isArray(p.priceHistory) ? p.priceHistory : (Array.isArray(p.history) ? p.history : []),
          stock: p.stock,
          confidence: p.confidence,
          history: Array.isArray(p.history) ? p.history : (Array.isArray(p.priceHistory) ? p.priceHistory : []),
          _raw: p || null,
        }));
        if (!mounted) return;
        setPlayers(shaped);
        // log counts for debugging in the browser console
        try {
          // eslint-disable-next-line no-console
          console.info(`[PlayersVirtualGrid] fetched ${shaped.length} players, ${shaped.filter((pl: PlayerShape) => Array.isArray(pl.priceHistory) && pl.priceHistory.length).length} have priceHistory`);
        } catch (e) {}
      } catch (err) {
        // ignore and show empty list
      } finally {
        if (mounted) setLoading(false);
      }
    })();
    return () => { mounted = false; };
  }, [initialPlayers]);

  // Poll live updates every 10 minutes: call live WR updater and refresh roster
  useEffect(() => {
    let mounted = true;
    const refreshPlayers = async () => {
      try {
        // call live updater (harmless if it returns error) then re-fetch roster
        await fetch('/api/live/wr').catch(() => null);
        const search = (typeof window !== 'undefined' && window.location.search) ? window.location.search : '';
        const params = new URLSearchParams(search);
        if (!params.has('all')) params.set('all', '1');
        const q = params.toString();
        const res = await fetch(`/api/nfl/stocks?${q}`);
        const json = await res.json();
        const list = Array.isArray(json?.players) ? json.players : [];
        const shaped = list.map((p: any) => ({
          espnId: String(p.espnId || p.id || ''),
          name: p.name || p.player || '',
          position: p.position || p.position_profile || '',
          team: p.team || '',
          priceHistory: Array.isArray(p.priceHistory) ? p.priceHistory : (Array.isArray(p.history) ? p.history : []),
          stock: p.stock,
          confidence: p.confidence,
          history: Array.isArray(p.history) ? p.history : (Array.isArray(p.priceHistory) ? p.priceHistory : []),
          _raw: p || null,
        }));
        if (!mounted) return;
        setPlayers(shaped);
      } catch (err) {
        // swallow errors; we'll retry on the next interval
      }
    };

    const id = window.setInterval(() => { void refreshPlayers(); }, 600000);
    // run one immediate refresh after mount
    void refreshPlayers();
    return () => { mounted = false; window.clearInterval(id); };
  }, []);

  // If initial players were passed in, log quick stats for debugging
  useEffect(() => {
    if (initialPlayers && initialPlayers.length) {
      try {
        // eslint-disable-next-line no-console
        console.info(`[PlayersVirtualGrid] initial players prop: ${initialPlayers.length}, with ${initialPlayers.filter((p: PlayerShape) => Array.isArray(p.priceHistory) && p.priceHistory.length).length} having priceHistory`);
      } catch (e) {}
    }
  }, [initialPlayers]);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [columns, setColumns] = useState(4);
  const [width, setWidth] = useState(1200);
  const [listHeight, setListHeight] = useState<number>(800);

  // Simple responsive breakpoints to match the original CSS grid behavior
  const computeColumns = useCallback((w: number) => {
    if (w < 640) return 1; // sm -> 1
    if (w < 900) return 2; // md small
    if (w < 1200) return 3; // md/large
    return 4; // large
  }, []);

  useEffect(() => {
    const update = () => {
      const w = containerRef.current?.clientWidth || (typeof window !== 'undefined' ? window.innerWidth : 1200);
      setWidth(w);
      setColumns(computeColumns(w));
      // compute and set list height on the client only (keep initial SSR value stable)
      const h = (typeof window !== 'undefined') ? Math.max(600, window.innerHeight - 220) : 800;
      setListHeight(h);
      // reset virtualization cache when columns change (only if method exists).
      // Some react-window list refs expose `resetAfterIndex` (VariableSizeList)
      // while FixedSizeList may not — guard the call to avoid runtime errors.
      try {
        listRef.current?.resetAfterIndex?.(0, true);
      } catch (e) {
        // fallback: if resetAfterIndex isn't available, attempt a gentle scroll reset
        try {
          listRef.current?.scrollToItem?.(0);
        } catch (err) {
          // swallow any errors — this is non-critical
        }
      }
    };
    update();
    window.addEventListener('resize', update);
    return () => window.removeEventListener('resize', update);
  }, [computeColumns]);

  // virtualization: each row contains `columns` cards
  const rowCount = Math.max(0, Math.ceil(players.length / columns));

  // Performance logging: measure frames while scrolling and log a short summary
  const scrollingRef = useRef(false);
  const rafRef = useRef<number | null>(null);
  const framesRef = useRef(0);
  const startTimeRef = useRef<number | null>(null);
  const lastScrollTsRef = useRef<number>(0);
  const scrollEndTimeoutRef = useRef<number | null>(null);

  const startFrameLoop = () => {
    if (rafRef.current != null) return;
    framesRef.current = 0;
    startTimeRef.current = performance.now();
    const loop = (t: number) => {
      framesRef.current++;
      rafRef.current = requestAnimationFrame(loop);
    };
    rafRef.current = requestAnimationFrame(loop);
  };
  const stopFrameLoopAndLog = () => {
    if (rafRef.current != null) cancelAnimationFrame(rafRef.current);
    rafRef.current = null;
    const start = startTimeRef.current ?? performance.now();
    const duration = Math.max(1, (performance.now() - start) / 1000);
    const frames = framesRef.current || 0;
    const fps = frames / duration;
    // simple console summary
    // eslint-disable-next-line no-console
    console.info(`[PlayersVirtualGrid] scroll summary — duration: ${duration.toFixed(2)}s, frames: ${frames}, approx FPS: ${fps.toFixed(1)}, columns: ${columns}, totalPlayers: ${players.length}`);
    framesRef.current = 0;
    startTimeRef.current = null;
  };

  const handleScroll = ({ scrollDirection }: ListOnScrollProps) => {
    // start/refresh the frame measurement loop
    lastScrollTsRef.current = performance.now();
    if (!scrollingRef.current) {
      scrollingRef.current = true;
      startFrameLoop();
    }
    // debounce end
    if (scrollEndTimeoutRef.current) window.clearTimeout(scrollEndTimeoutRef.current);
    scrollEndTimeoutRef.current = window.setTimeout(() => {
      scrollingRef.current = false;
      stopFrameLoopAndLog();
    }, 200);
  };

  // Row renderer: builds a horizontal row of `columns` PlayerCards
  const Row = ({ index, style }: { index: number; style: React.CSSProperties }) => {
    const start = index * columns;
    const end = Math.min(start + columns, players.length);
    const rowPlayers = players.slice(start, end);
    return (
      <div style={{ ...style, display: 'flex', gap: 12, padding: '8px 6px', boxSizing: 'border-box' }}>
        {rowPlayers.map((p) => (
          <div key={p.espnId} style={{ flex: 1, minWidth: 0 }}>
            <a href={`/players/${p.espnId}`} aria-label={`Open ${p.name ?? 'player'} details`}>
              <PlayerCard
                player={{
                  id: p.espnId,
                  name: p.name ?? p._raw?.player ?? 'Unknown',
                  position: p.position,
                  espnId: p.espnId,
                  team: p.team,
                  priceHistory: p.priceHistory ?? p.history ?? [],
                  stock: p.stock,
                  confidence: p.confidence,
                  history: p.history ?? p.priceHistory ?? [],
                  _raw: p._raw ?? null,
                }}
              />
            </a>
          </div>
        ))}
        {/* Fill empty columns so spacing stays consistent */}
        {Array.from({ length: columns - rowPlayers.length }).map((_, i) => (
          <div key={`empty-${i}`} style={{ flex: 1 }} />
        ))}
      </div>
    );
  };

  // compute list width/height (listHeight is stateful to avoid SSR/client hydration mismatch)
  const listWidth = width;

  return (
    <div ref={containerRef} style={{ width: '100%' }}>
      <List
        ref={(r: any) => (listRef.current = r)}
        height={listHeight}
        width={listWidth}
        itemCount={rowCount}
        itemSize={rowHeight}
        onScroll={handleScroll}
        overscanCount={5}
      >
        {Row}
      </List>
    </div>
  );
}
