export type Position =
  | 'QB'
  | 'RB'
  | 'WR'
  | 'TE'
  | 'OL'
  | 'DL'
  | 'LB'
  | 'S'
  | 'CB'
  | 'K'
  | 'P'
  | string;

// Tuned multipliers (less aggressive than before) — these are multiplicative on the
// computed stat score (not the base price). We'll use them to bias QBs/RBs/WRs slightly.
export const POSITION_MULTIPLIERS: Record<string, number> = {
  QB: 1.25,
  WR: 1.15,
  RB: 1.1,
  TE: 1.05,
  DEF: 0.9,
};

export function posMultiplier(pos?: string): number {
  if (!pos) return 1;
  const p = String(pos).toUpperCase();
  for (const key of Object.keys(POSITION_MULTIPLIERS)) {
    if (p.includes(key)) return POSITION_MULTIPLIERS[key] ?? 1;
  }
  return 1;
}

// Position-specific stat weights (how much each stat contributes to the score)
export const POSITION_WEIGHTS: Record<string, { yards?: number; rec?: number; rush?: number; tds?: number; ints?: number; fumbles?: number }> = {
  QB: { yards: 0.005, tds: 6, ints: -2, fumbles: -2 },
  WR: { rec: 0.3, yards: 0.1, tds: 6, fumbles: -1 },
  RB: { rush: 0.1, rec: 0.2, yards: 0.05, tds: 6, fumbles: -1 },
  TE: { rec: 0.25, yards: 0.08, tds: 6, fumbles: -1 },
  DEF: { tds: 2, ints: 2 },
};

function getWeightsForPosition(pos?: string) {
  if (!pos) return { yards: 0.05, tds: 10, ints: -8, fumbles: -8 };
  const p = String(pos).toUpperCase();
  for (const key of Object.keys(POSITION_WEIGHTS)) {
    if (p.includes(key)) return POSITION_WEIGHTS[key];
  }
  return { yards: 0.05, tds: 10, ints: -8, fumbles: -8 };
}

export function computeInitialPriceFromStats(
  stats: {
    yards?: number;
    tds?: number;
    ints?: number;
    fumbles?: number;
    rec?: number;
    rush?: number;
  } | null,
  position?: string,
  base = 80
) {
  const w = getWeightsForPosition(position);

  const yards = stats?.yards ?? 0;
  const rec = stats?.rec ?? 0;
  const rush = stats?.rush ?? 0;
  const tds = stats?.tds ?? 0;
  const ints = stats?.ints ?? 0;
  const fumbles = stats?.fumbles ?? 0;

  // Compute a weighted score according to position-specific weights
  let score = 0;
  if (w.yards) score += yards * w.yards;
  if (w.rec) score += rec * w.rec;
  if (w.rush) score += rush * w.rush;
  if (w.tds) score += tds * w.tds;
  if (w.ints) score += ints * w.ints;
  if (w.fumbles) score += fumbles * w.fumbles;

  const multiplier = posMultiplier(position);
  const raw = base + score * multiplier;

  // Clamp to reasonable bounds
  const clamped = Math.max(5, Math.min(2000, raw));
  return Number(clamped.toFixed(2));
}

// --- New: Performance scoring helpers (implements formulas provided by user)

export type QBStats = {
  EPA_per_play?: number;
  CPOE?: number;
  ANY_A?: number;
  passingYards?: number;
  passingTDs?: number;
  interceptions?: number;
};

export type RBStats = {
  RushYardsOverExpected_per_Att?: number;
  SuccessRate?: number;
  YAC_per_Att?: number;
  rushingYards?: number;
  rushingTDs?: number;
  receivingYards?: number;
};

export type WRStats = {
  YardsPerRouteRun?: number;
  CatchRateOverExpected?: number;
  EPA_per_Target?: number;
  receivingYards?: number;
  receivingTDs?: number;
  receptions?: number;
};

export type DEFStats = {
  EPA_Allowed_per_Play?: number;
  SuccessRateAllowed?: number;
  sacks?: number;
  turnovers?: number;
  pointsAllowedAdjustment?: number;
};

export function clampNum(v: number | undefined | null, fallback = 0) {
  if (v == null || Number.isNaN(Number(v))) return fallback;
  return Number(v);
}

// Compute the position-specific composite score as per the formulas given.
export function computePerformanceScore(position: string | undefined, stats: any): number {
  const pos = (position || '').toUpperCase();

  if (pos.includes('QB')) {
    const EPA_per_play = clampNum(stats?.EPA_per_play);
    const CPOE = clampNum(stats?.CPOE);
    const ANY_A = clampNum(stats?.ANY_A);
    const passingYards = clampNum(stats?.passingYards);
    const passingTDs = clampNum(stats?.passingTDs);
    const interceptions = clampNum(stats?.interceptions);

    const advanced = 0.5 * EPA_per_play + 0.3 * CPOE + 0.2 * ANY_A;
    const traditional = (passingYards / 300) + (passingTDs * 0.75) - (interceptions * 0.5);
    return 0.6 * advanced + 0.4 * traditional;
  }

  if (pos.includes('RB')) {
    const ROE = clampNum(stats?.RushYardsOverExpected_per_Att);
    const SuccessRate = clampNum(stats?.SuccessRate);
    const YAC_per_Att = clampNum(stats?.YAC_per_Att);
    const rushingYards = clampNum(stats?.rushingYards);
    const rushingTDs = clampNum(stats?.rushingTDs);
    const receivingYards = clampNum(stats?.receivingYards);

    const advanced = 0.4 * ROE + 0.3 * SuccessRate + 0.3 * YAC_per_Att;
    const traditional = (rushingYards / 100) + (rushingTDs * 0.8) + (receivingYards / 50);
    return 0.6 * advanced + 0.4 * traditional;
  }

  if (pos.includes('WR')) {
    const YPRR = clampNum(stats?.YardsPerRouteRun);
    const CROE = clampNum(stats?.CatchRateOverExpected);
    const EPA_per_Target = clampNum(stats?.EPA_per_Target);
    const receivingYards = clampNum(stats?.receivingYards);
    const receivingTDs = clampNum(stats?.receivingTDs);
    const receptions = clampNum(stats?.receptions);

    const advanced = 0.4 * YPRR + 0.3 * CROE + 0.3 * EPA_per_Target;
    const traditional = (receivingYards / 100) + (receivingTDs * 0.8) + (receptions / 10);
    return 0.6 * advanced + 0.4 * traditional;
  }

  if (pos.includes('D') || pos.includes('DEF') || pos.includes('SAF') || pos.includes('CB') || pos.includes('DL') || pos.includes('LB')) {
    const EPA_Allowed_per_Play = clampNum(stats?.EPA_Allowed_per_Play);
    const SuccessRateAllowed = clampNum(stats?.SuccessRateAllowed);
    const sacks = clampNum(stats?.sacks);
    const turnovers = clampNum(stats?.turnovers);
    const pointsAllowedAdjustment = clampNum(stats?.pointsAllowedAdjustment);

    const advanced = 0.5 * (EPA_Allowed_per_Play * -1) + 0.5 * (SuccessRateAllowed * -1);
    const traditional = (sacks * 0.5) + (turnovers * 1.0) + pointsAllowedAdjustment;
    return 0.6 * advanced + 0.4 * traditional;
  }

  // Fallback: attempt to derive a simple score from generic stats (yards/tds)
  const yards = clampNum(stats?.yards);
  const tds = clampNum(stats?.tds);
  const ints = clampNum(stats?.ints);
  const fumbles = clampNum(stats?.fumbles);
  const fallbackScore = (yards * 0.02) + (tds * 2) - (ints + fumbles) * 1.5;
  return fallbackScore;
}

// Compute performanceFactor and newPrice with tanh scaling. leagueAvgScore can be provided
// via environment overrides; sensitivity controls how strongly score affects price.
export function computePerformanceFactor(score: number, leagueAvgPositionScore = 1) {
  if (!leagueAvgPositionScore || Number.isNaN(leagueAvgPositionScore)) leagueAvgPositionScore = 1;
  return (score / leagueAvgPositionScore) - 1;
}

export function computePriceFromPerformance(oldPrice: number, performanceFactor: number, sensitivity = 1) {
  const adj = Math.tanh(performanceFactor * sensitivity);
  const candidate = oldPrice * (1 + adj);
  return Number(Math.max(0.01, Number(candidate.toFixed(2))));
}

