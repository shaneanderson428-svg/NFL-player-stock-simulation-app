export type PricePoint = {
  t: string; // time
  p: number; // price
  v?: number; // optional volume
  e?: { type: string; impact?: number } | null; // optional event metadata
};

export type Athlete = {
  id: string;
  name: string;
  team: string;
  sport: 'Football' | 'Basketball' | 'Baseball' | 'Soccer';
  position: string;
  // position provided by cleaned profile data (if available)
  position_profile?: string;
  // whether the profile value was used to overwrite the inferred/UNK position
  position_overwritten_from_profile?: boolean;
  // position inferred by the aggregator when profile not available
  position_inferred?: string;
  currentPrice: number;
  previousPrice: number;
  marketCap: number;
  sharesOwned?: number;
  totalShares?: number;
  priceHistory: PricePoint[];
  imageUrl?: string;
};
