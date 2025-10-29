import type { Athlete } from "./types";

const now = new Date();
const days = (n: number) => {
  const d = new Date(now);
  d.setDate(d.getDate() - n);
  return d.toISOString().slice(0, 10);
};

const history = (base: number): Athlete["priceHistory"] =>
  Array.from({ length: 14 }, (_, i) => {
    const drift = Math.sin(i / 2) * 4;       // smooth wave-like price pattern
    const noise = Math.random() * 3 - 1.5;   // random small changes
    return { t: days(13 - i), p: +(base + drift + noise).toFixed(2) };
  });

export const MOCK_ATHLETES: Athlete[] = [
  {
    id: "pm15",
    name: "Patrick Mahomes",
    team: "Kansas City Chiefs",
    sport: "Football",
    position: "Quarterback",
    currentPrice: 411.05,
    previousPrice: 398.84,
    marketCap: 42500000,
    sharesOwned: 0,
    totalShares: 100000,
    priceHistory: history(405),
    imageUrl: "https://upload.wikimedia.org/wikipedia/commons/2/2c/Patrick_Mahomes_2021.jpg",
  },
  {
    id: "tk87",
    name: "Travis Kelce",
    team: "Kansas City Chiefs",
    sport: "Football",
    position: "Tight End",
    currentPrice: 289.12,
    previousPrice: 285.40,
    marketCap: 27500000,
    sharesOwned: 0,
    totalShares: 100000,
    priceHistory: history(285),
    imageUrl: "https://upload.wikimedia.org/wikipedia/commons/7/7f/Travis_Kelce_2021.jpg",
  },
  {
    id: "ty10",
    name: "Tyreek Hill",
    team: "Miami Dolphins",
    sport: "Football",
    position: "Wide Receiver",
    currentPrice: 372.64,
    previousPrice: 365.10,
    marketCap: 39500000,
    sharesOwned: 0,
    totalShares: 100000,
    priceHistory: history(368),
    imageUrl: "https://upload.wikimedia.org/wikipedia/commons/0/07/Tyreek_Hill_2021.jpg",
  },
];
