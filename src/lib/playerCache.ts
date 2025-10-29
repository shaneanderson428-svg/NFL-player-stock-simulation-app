type PlayerEntry = {
  espnId: string;
  name?: string;
  team?: string;
  position?: string;
  history?: Array<Record<string, any>>;
  [k: string]: any;
};

const cache = new Map<string, PlayerEntry>();

export function getOrInitPlayer(espnId: string, base?: Partial<PlayerEntry>) {
  const key = String(espnId || '').trim();
  if (!key) return null;
  let entry = cache.get(key);
  if (!entry) {
    entry = {
      espnId: key,
      name: base?.name ?? base?.player ?? '',
      team: base?.team ?? '',
      position: (base?.position ?? '') as string,
      history: Array.isArray(base?.history) ? [...(base!.history as any[])] : [],
      ...base,
    } as PlayerEntry;
    cache.set(key, entry);
    return entry;
  }

  // Merge some sensible fields from base into existing entry without overwriting history
  try {
    if (base?.name) entry.name = base.name;
    if (base?.team) entry.team = base.team;
    if (base?.position) entry.position = base.position;
    // keep existing history but allow adding new weeks
    if (Array.isArray(base?.history) && base.history.length > 0) {
      const existingWeeks = new Set((entry.history || []).map((h: any) => String(h.week ?? '').trim()));
      for (const h of base.history as any[]) {
        const wk = String(h.week ?? '').trim();
        if (!existingWeeks.has(wk)) {
          (entry.history ||= []).push(h);
          existingWeeks.add(wk);
        }
      }
  // keep history sorted by week if numeric
  entry.history = entry.history || [];
  entry.history.sort((a: any, b: any) => Number(a.week ?? 0) - Number(b.week ?? 0));
    }
  } catch (e) {
    // ignore merge errors
  }

  return entry;
}

export function listAllPlayers() {
  return Array.from(cache.values());
}

export function clearCache() {
  cache.clear();
}

export default { getOrInitPlayer, listAllPlayers, clearCache };
