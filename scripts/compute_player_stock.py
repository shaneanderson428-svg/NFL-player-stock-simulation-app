"""Compute QB stock from per-game player stats.

Reads a per-game CSV (default: data/player_game_stats.csv), computes
league-wide z-scores and a stock metric per game, then writes a per-player
summary CSV and a per-player history CSV. The summary always contains a
stable `pass_attempts` column so downstream code can reliably filter on
raw attempt counts.
"""

import argparse
from pathlib import Path
import sys
import math

import pandas as pd
from typing import Optional, Any


# Helper: apply volatility multipliers and compute adjusted/capped changes and new prices.
def apply_volatility_multiplier(
    result: pd.DataFrame, price_col: Optional[str] = None
) -> pd.DataFrame:
    """Given a result DataFrame (with a 'weekly_change' column), compute a
    volatility multiplier per-row and produce diagnostic columns:
      - rawChange: original weekly_change (copied)
      - multiplier: applied multiplier
      - cappedChange: final capped change after multiplier
      - newPrice: computed when price_col provided

    Multipliers (precedence: playoff > primetime > gameday > rest):
      rest/off days: 1.0
      regular gamedays: 1.5
      primetime: 1.75
      playoffs / Super Bowl: 2.0
    """
    # Work on a copy to avoid surprising callers
    res = result

    # Ensure weekly_change exists
    if "weekly_change" not in res.columns:
        res["weekly_change"] = 0.0

    # Start multiplier at 1.0
    multiplier = pd.Series(1.0, index=res.index)

    # Determine gameday via week or __game_date
    try:
        if "week" in res.columns:
            wk_ser = (
                pd.to_numeric(pd.Series(res["week"]), errors="coerce")
                .fillna(0)
                .astype(int)
            )
        else:
            wk_ser = pd.Series(0, index=res.index)
    except Exception:
        wk_ser = pd.Series(0, index=res.index)

    has_game = wk_ser > 0
    try:
        if "__game_date" in res.columns:
            has_game = has_game | res["__game_date"].notna()
    except Exception:
        pass

    multiplier.loc[has_game] = 1.5

    # Playoff / Super Bowl detection
    playoff_mask = pd.Series(False, index=res.index)
    if "is_playoff" in res.columns:
        try:
            playoff_mask = playoff_mask | res["is_playoff"].astype(bool)
        except Exception:
            try:
                playoff_mask = playoff_mask | (
                    pd.to_numeric(pd.Series(res["is_playoff"]), errors="coerce").fillna(
                        0
                    )
                    > 0
                )
            except Exception:
                pass
    if "game_type" in res.columns:
        try:
            gt = res["game_type"].astype(str).str.lower()
            playoff_mask = (
                playoff_mask | gt.str.contains("post") | gt.str.contains("playoff")
            )
        except Exception:
            pass
    if "event" in res.columns:
        try:
            ev = res["event"].astype(str).str.lower()
            playoff_mask = (
                playoff_mask | ev.str.contains("super") | ev.str.contains("super bowl")
            )
        except Exception:
            pass
    multiplier.loc[playoff_mask] = 2.0

    # Primetime heuristics/flags
    primetime_mask = pd.Series(False, index=res.index)
    for col in ("is_primetime", "primetime", "primetime_flag"):
        if col in res.columns:
            try:
                primetime_mask = primetime_mask | res[col].astype(bool)
            except Exception:
                try:
                    primetime_mask = primetime_mask | (
                        pd.to_numeric(pd.Series(res[col]), errors="coerce").fillna(0)
                        > 0
                    )
                except Exception:
                    pass

    try:
        if "__game_date" in res.columns:
            wd = pd.to_datetime(
                pd.Series(res["__game_date"]), errors="coerce"
            ).dt.weekday
            primetime_mask = primetime_mask | wd.isin([0, 3, 6])
    except Exception:
        pass

    for time_col in ("kickoff_time", "game_time", "start_time", "kickoff"):
        if time_col in res.columns:
            try:
                hours = pd.to_datetime(
                    pd.Series(res[time_col]), errors="coerce"
                ).dt.hour
                primetime_mask = primetime_mask | (hours >= 18) & (hours <= 23)
            except Exception:
                pass

    if primetime_mask.any():
        multiplier.loc[primetime_mask] = multiplier.loc[primetime_mask].apply(
            lambda v: max(v, 1.75)
        )

    # ----- New fields: trading_volume and sentiment_score -----
    # Coerce trading_volume to numeric (default 0)
    if "trading_volume" in res.columns:
        try:
            tv = pd.to_numeric(pd.Series(res["trading_volume"]), errors="coerce").fillna(0)
        except Exception:
            tv = pd.Series(0, index=res.index)
    else:
        tv = pd.Series(0, index=res.index)
    # store normalized trading_volume
    res["trading_volume"] = tv

    # Coerce sentiment_score to numeric (default 0.0)
    if "sentiment_score" in res.columns:
        try:
            sscore = pd.to_numeric(pd.Series(res["sentiment_score"]), errors="coerce").fillna(0.0)
        except Exception:
            sscore = pd.Series(0.0, index=res.index)
    else:
        sscore = pd.Series(0.0, index=res.index)
    res["sentiment_score"] = sscore.round(4)

    # sentiment factor: scale by magnitude, clamped between 0.8 and 1.2
    # sentiment_factor = 1 + 0.1 * sentiment_score
    try:
        sentiment_factor = 1.0 + (0.1 * sscore)
        # clamp
        sentiment_factor = sentiment_factor.clip(lower=0.8, upper=1.2)
    except Exception:
        sentiment_factor = pd.Series(1.0, index=res.index)
    res["sentiment_factor"] = sentiment_factor.round(4)

    # Compute adjusted and capped changes
    # Combine gameday multiplier and sentiment multiplicatively
    try:
        total_multiplier = multiplier * sentiment_factor
    except Exception:
        # fall back to elementwise multiplication if necessary
        total_multiplier = pd.Series(1.0, index=res.index)
        for i in res.index:
            try:
                total_multiplier.at[i] = float(multiplier.at[i]) * float(sentiment_factor.at[i])
            except Exception:
                total_multiplier.at[i] = float(multiplier.at[i])

    # raw_change is the change after combining multiplier and sentiment
    raw_change = res["weekly_change"] * total_multiplier

    # Off-season mask: not gameday and not playoff -> rest/off day
    offseason_mask = pd.Series(False, index=res.index)
    if "is_gameday" in res.columns:
        try:
            offseason_mask = (~res["is_gameday"].astype(bool))
        except Exception:
            try:
                offseason_mask = (
                    pd.to_numeric(pd.Series(res["is_gameday"]), errors="coerce").fillna(0)
                    == 0
                )
            except Exception:
                offseason_mask = pd.Series(False, index=res.index)
    # ensure playoffs are excluded from decay
    offseason_mask = offseason_mask & (~playoff_mask)

    # Apply ±20% cap to total price change per update
    capped_change = raw_change.clip(lower=-0.20, upper=0.20)

    # Persist diagnostic columns requested by QA
    # Keep rawChange as the original weekly_change for traceability
    res["rawChange"] = res["weekly_change"].round(6)
    # gameday multiplier (pre-sentiment)
    res["multiplier"] = multiplier.round(3)
    # Save sentiment and trading diagnostics
    res["trading_volume"] = tv
    res["sentiment_score"] = sscore.round(4)
    res["sentiment_factor"] = sentiment_factor.round(3)
    # Record whether off-season decay was applied and the decay amount
    res["offseason_decay_applied"] = offseason_mask
    res["cappedChange"] = capped_change.round(6)

    # Backwards-compatible names (kept for other code/tests)
    res["volatility_multiplier"] = res["multiplier"]
    # applied_weekly_change reflects the change after multiplier and sentiment (pre-cap)
    res["applied_weekly_change"] = raw_change.round(6)
    res["applied_weekly_change_capped"] = res["cappedChange"]

    # Compute newPrice when price column provided
    if price_col and price_col in res.columns:
        try:
            price_series = pd.to_numeric(
                pd.Series(res[price_col]), errors="coerce"
            ).fillna(0.0)
            # Compute new price using capped change
            new_price_series = (price_series * (1.0 + capped_change)).round(6)
            # Apply off-season multiplicative decay (0.995) to final price where applicable
            try:
                if offseason_mask.any():
                    new_price_series.loc[offseason_mask] = (
                        new_price_series.loc[offseason_mask] * 0.995
                    )
            except Exception:
                # fallback: elementwise
                for i in new_price_series.index[offseason_mask]:
                    try:
                        new_price_series.at[i] = new_price_series.at[i] * 0.995
                    except Exception:
                        pass

            res["newPrice"] = new_price_series.round(4)
            res["new_price"] = res["newPrice"]
        except Exception:
            res["newPrice"] = ""
            res["new_price"] = ""

    return res


# Small helpers to safely coerce potentially-Any/None values to numeric scalars
def _safe_to_numeric_scalar(x):
    try:
        return pd.to_numeric(pd.Series([x]), errors="coerce").iloc[0]
    except Exception:
        return float("nan")


def _safe_float(x, default=0.0):
    v = _safe_to_numeric_scalar(x)
    return float(v) if not pd.isna(v) else default


def _safe_float_maybe(x):
    v = _safe_to_numeric_scalar(x)
    return float(v) if not pd.isna(v) else ""


def _safe_int(x, default=0):
    v = _safe_to_numeric_scalar(x)
    try:
        return int(v) if not pd.isna(v) else default
    except Exception:
        return default


def compute_stock_qb(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    numcols = [
        "pass_yards",
        "pass_tds",
        "ints",
        "rush_yards",
        "rush_tds",
        "fumbles",
        "epa_per_play",
        "cpoe",
        "pass_attempts",
    ]
    for c in numcols:
        # Ensure we always pass a Series to to_numeric (df.get may return a scalar)
        if c in df.columns:
            # Wrap the input in Series to satisfy static type checkers that
            # may treat df[...] as Any or scalar in some contexts.
            series = pd.to_numeric(pd.Series(df[c]), errors="coerce").fillna(0.0)
        else:
            series = pd.Series(0.0, index=df.index)
        df[c] = series

    z_cols = {}
    for c in numcols:
        mean = df[c].mean()
        std = df[c].std()
        if std is None or std == 0 or pd.isna(std):
            df[f"z_{c}"] = 0.0
        else:
            df[f"z_{c}"] = ((df[c] - mean) / std).clip(-6.0, 6.0)
        z_cols[c] = f"z_{c}"

    df["B"] = (
        0.10 * df[z_cols["pass_yards"]]
        + 0.15 * df[z_cols["pass_tds"]]
        - 0.15 * df[z_cols["ints"]]
        + 0.20 * df[z_cols["rush_yards"]]
        + 0.25 * df[z_cols["rush_tds"]]
        - 0.05 * df[z_cols["fumbles"]]
    )

    df["M"] = 1.0 + df[z_cols["epa_per_play"]] + df[z_cols["cpoe"]]
    df["C"] = (
        (df["pass_attempts"] / 20.0)
        .apply(lambda v: math.sqrt(v) if v > 0 else 0.0)
        .clip(upper=1.0)
    )
    df["stock"] = 100.0 + 10.0 * df["C"] * df["B"] * df["M"]

    df["stock"] = df["stock"].round(4)
    for k in z_cols.values():
        df[k] = df[k].round(4)
    df["C"] = df["C"].round(4)
    return df


def aggregate_pbp_files(pbp_dir: Path) -> pd.DataFrame:
    """Read play-by-play CSVs from pbp_dir and aggregate per-player per-game stats.

    This is heuristic-based to support common nflfastR column names. It returns
    a DataFrame with one row per player+game_date containing columns used by
    compute_stock_qb (pass_yards, pass_tds, ints, rush_yards, rush_tds,
    fumbles, epa_per_play, cpoe, pass_attempts, player, week).
    """
    files = list(pbp_dir.glob("*.csv")) + list(pbp_dir.glob("*.csv.gz"))
    if not files:
        return pd.DataFrame()

    # accumulator keyed by (player, game_date)
    acc = {}

    def add_stat(key, k, v):
        if key not in acc:
            acc[key] = {
                "player": key[0],
                "game_date": key[1],
                "plays": 0,
                "epa_sum": 0.0,
                "epa_count": 0,
                "cpoe_sum": 0.0,
                "cpoe_count": 0,
            }
        acc[key][k] = acc[key].get(k, 0) + v

    for f in files:
        try:
            pbp = pd.read_csv(f, compression="infer", low_memory=False)
        except Exception:
            continue
        # normalize column lookups
        cols = {c.lower(): c for c in pbp.columns}
        # possible name columns
        passer_col = cols.get("passer_player_name")
        rusher_col = cols.get("rusher_player_name")
        receiver_col = cols.get("receiver_player_name") or cols.get(
            "reciever_player_name"
        )
        epa_col = cols.get("epa")
        cpoe_col = cols.get("cpoe")
        date_col = cols.get("game_date") or cols.get("date")
        pass_att_col = cols.get("pass_attempt") or cols.get("pass_attempts")
        pass_y_col = (
            cols.get("passer_yards")
            or cols.get("pass_yards")
            or cols.get("yards_gained")
        )
        rush_y_col = (
            cols.get("rusher_yards")
            or cols.get("rush_yards")
            or cols.get("yards_gained")
        )
        int_col = cols.get("interception") or cols.get("interceptions")
        pass_td_col = (
            cols.get("pass_touchdown")
            or cols.get("pass_td")
            or cols.get("pass_touchdowns")
        )
        rush_td_col = (
            cols.get("rush_touchdown")
            or cols.get("rush_td")
            or cols.get("rush_touchdowns")
        )
        fumble_col = cols.get("fumble") or cols.get("fumble_lost")

        for _, r in pbp.iterrows():
            # determine game date
            gdate = None
            if date_col and pd.notna(r.get(date_col)):
                try:
                    # Use Series-based to_datetime to satisfy type checkers and
                    # coerce invalid values to NaT safely.
                    raw_val = r.get(date_col)
                    parsed = pd.to_datetime(pd.Series([raw_val]), errors="coerce").iloc[
                        0
                    ]
                    if not pd.isna(parsed):
                        gdate = parsed.strftime("%Y-%m-%d")
                    else:
                        gdate = str(raw_val)
                except Exception:
                    gdate = str(r.get(date_col))
            else:
                gdate = ""

            # helper to process a player name for this play
            def proc_player(name, role):
                if not name or pd.isna(name):
                    return
                pname = str(name).strip()
                key = (pname, gdate)
                # base counts
                # EPA
                if epa_col and pd.notna(r.get(epa_col)):
                    try:
                        raw = r.get(epa_col)
                        e = _safe_float(raw, default=float("nan"))
                        if not pd.isna(e):
                            add_stat(key, "epa_sum", float(e))
                            add_stat(key, "epa_count", 1)
                    except Exception:
                        pass
                # CPOE
                if cpoe_col and pd.notna(r.get(cpoe_col)):
                    try:
                        raw = r.get(cpoe_col)
                        c = _safe_float(raw, default=float("nan"))
                        if not pd.isna(c):
                            add_stat(key, "cpoe_sum", float(c))
                            add_stat(key, "cpoe_count", 1)
                    except Exception:
                        pass
                # role-specific
                if role == "passer":
                    # pass attempt
                    if pass_att_col and pd.notna(r.get(pass_att_col)):
                        try:
                            pa = _safe_int(r.get(pass_att_col), default=1)
                            add_stat(key, "pass_attempts", pa)
                        except Exception:
                            add_stat(key, "pass_attempts", 1)
                    else:
                        # if a passer is recorded, count as an attempt
                        add_stat(key, "pass_attempts", 1)
                    # pass yards
                    if pass_y_col and pd.notna(r.get(pass_y_col)):
                        try:
                            py = _safe_float(r.get(pass_y_col), default=float("nan"))
                            if not pd.isna(py):
                                add_stat(key, "pass_yards", float(py))
                        except Exception:
                            pass
                    # pass TD
                    if pass_td_col and r.get(pass_td_col):
                        add_stat(key, "pass_tds", 1)
                    # interception
                    if int_col and r.get(int_col):
                        add_stat(key, "ints", 1)
                if role == "rusher":
                    if rush_y_col and pd.notna(r.get(rush_y_col)):
                        try:
                            ry = _safe_float(r.get(rush_y_col), default=float("nan"))
                            if not pd.isna(ry):
                                add_stat(key, "rush_yards", float(ry))
                        except Exception:
                            pass
                    if rush_td_col and r.get(rush_td_col):
                        add_stat(key, "rush_tds", 1)
                # fumbles
                if fumble_col and r.get(fumble_col):
                    add_stat(key, "fumbles", 1)

            # process known roles
            proc_player(r.get(passer_col) if passer_col else None, "passer")
            proc_player(r.get(rusher_col) if rusher_col else None, "rusher")
            proc_player(r.get(receiver_col) if receiver_col else None, "receiver")

    # convert acc to DataFrame
    rows = []
    for (pname, gdate), vals in acc.items():
        row = {
            "player": pname,
            "week": "",
            "espnId": "",
        }
        # aggregate sums
        row["pass_attempts"] = vals.get("pass_attempts", 0)
        row["pass_yards"] = vals.get("pass_yards", 0)
        row["pass_tds"] = vals.get("pass_tds", 0)
        row["ints"] = vals.get("ints", 0)
        row["rush_yards"] = vals.get("rush_yards", 0)
        row["rush_tds"] = vals.get("rush_tds", 0)
        row["fumbles"] = vals.get("fumbles", 0)
        # epa / cpoe averages
        row["epa_per_play"] = (
            (vals.get("epa_sum", 0.0) / vals.get("epa_count", 1))
            if vals.get("epa_count", 0) > 0
            else 0.0
        )
        row["cpoe"] = (
            (vals.get("cpoe_sum", 0.0) / vals.get("cpoe_count", 1))
            if vals.get("cpoe_count", 0) > 0
            else 0.0
        )
        row["game_date"] = gdate
        rows.append(row)

    if not rows:
        return pd.DataFrame()
    outdf = pd.DataFrame(rows)
    # fill numeric NaNs
    for c in [
        "pass_attempts",
        "pass_yards",
        "pass_tds",
        "ints",
        "rush_yards",
        "rush_tds",
        "fumbles",
        "epa_per_play",
        "cpoe",
    ]:
        if c in outdf.columns:
            outdf[c] = pd.to_numeric(pd.Series(outdf[c]), errors="coerce").fillna(0)
    # try to set week from game_date if possible
    if "game_date" in outdf.columns:
        try:
            # Wrap game_date in a Series to avoid static typing issues
            outdf["week"] = (
                pd.to_datetime(pd.Series(outdf["game_date"]), errors="coerce")
                .dt.isocalendar()
                .week.fillna(0)
                .astype(int)
            )
        except Exception:
            outdf["week"] = 0

    return outdf


def summarize_latest(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values(["player", "week"])
    latest = df.groupby("player", as_index=False).last()
    zcols = [c for c in latest.columns if c.startswith("z_")]
    extra = ["espnId"] if "espnId" in latest.columns else []
    # include weekly_change and related diagnostic columns in latest summary when available
    extra_cols = [
        c
        for c in [
            "weekly_change",
            "weekly_change_pct",
            "rawChange",
            "multiplier",
            "cappedChange",
            "newPrice",
        ]
        if c in latest.columns
    ]
    outcols = ["player", "week", "stock", "C"] + extra_cols + extra + zcols
    out = latest[outcols].rename(columns={"week": "latest_week", "C": "confidence"})
    return out


def compute_player_stock_summary(input_path: str, output_path: str) -> pd.DataFrame:  # type: ignore
    inp = Path(input_path)
    outp = Path(output_path)
    if not inp.exists():
        raise FileNotFoundError(f"Input file {inp} not found")

    df = pd.read_csv(inp)

    # If play-by-play data exists, aggregate per-game player stats from it and
    # prefer that as the source of truth for game-level stock computation.
    pbp_dir = Path("data/pbp")
    try:
        if pbp_dir.exists():
            pbp_agg = aggregate_pbp_files(pbp_dir)
            if isinstance(pbp_agg, pd.DataFrame) and not pbp_agg.empty:
                # Use PBP-aggregated per-game stats as our dataframe
                df = pbp_agg
    except Exception:
        # on any pbp aggregation error, fall back to provided input CSV
        pass

    # Normalize common play-level column names to the canonical names used by the
    # rest of this script. In particular, play-by-play exports often have
    # `pass_attempt` per-play (0/1) while our code expects `pass_attempts`.
    # Make a best-effort mapping so both play-level and per-game inputs work.
    if "pass_attempt" in df.columns and "pass_attempts" not in df.columns:
        try:
            df["pass_attempts"] = pd.to_numeric(
                pd.Series(df["pass_attempt"]), errors="coerce"
            ).fillna(0)
        except Exception:
            df["pass_attempts"] = 0

    # Column aliases: map many common/dirty column names to canonical names used downstream.
    ALIASES = {
        # player/profile aliases
        "espnId": ["player_id", "playerid", "player_id", "id", "espnId", "espn_id"],
        "player": ["player_name", "player", "name", "playerName", "player_full_name"],
        "epa_per_play": [
            "epa/play",
            "epa_per_play",
            "epa_per_play",
            "epaPerPlay",
            "epa per play",
            "epa",
        ],
        "cpoe": ["cpoe", "cpoe_pct", "cpoe_percent"],
        "yards_per_attempt": [
            "yards_per_attempt",
            "yds_per_att",
            "ypa",
            "yards_per_pass_attempt",
        ],
        "td_int_ratio": ["td_int_ratio", "tds_per_int", "td_int", "td_int_ratio"],
        "rush_epa": ["rush_epa", "rush_epa_per_play", "rush_epa_play"],
        "yards_after_contact": [
            "yac",
            "yards_after_contact",
            "yards_after_contact_per_rush",
        ],
        "targets_per_game": [
            "targets_per_game",
            "targets",
            "targets_pg",
            "targets_per_g",
        ],
        "yards_per_route_run": ["yards_per_route_run", "yprr", "yds_per_route_run"],
        "target_share": ["target_share", "target_share_pct", "target_pct"],
        "yards_after_catch": ["yac", "yards_after_catch", "yac_per_target"],
        "tackles": ["tackles", "combined_tackles", "tkls"],
        "sacks": ["sacks", "sk"],
        "turnovers_forced": ["turnovers_forced", "forced_turnovers", "turnovers"],
    }

    # Build reverse lookup for alias -> canonical
    alias_reverse = {}
    for canon, variants in ALIASES.items():
        for v in variants:
            alias_reverse[v.lower().strip()] = canon

    # Normalize columns by renaming any column matching an alias (case-insensitive)
    rename_map = {}
    for col in df.columns:
        key = col.lower().strip()
        if key in alias_reverse:
            rename_map[col] = alias_reverse[key]
    if rename_map:
        df = df.rename(columns=rename_map)

    # Normalize possible ESPN id aliases to `espnId`.
    espn_aliases = [
        "espnId",
        "espn_id",
        "espn",
        "espnid",
        "playerId",
        "player_id",
        "playerid",
    ]
    col_map = {c.strip(): c for c in df.columns}
    lower_map = {c.strip().lower(): c for c in df.columns}
    found = None
    for a in espn_aliases:
        if a in col_map:
            found = col_map[a]
            break
        la = a.lower()
        if la in lower_map:
            found = lower_map[la]
            break
    if found:
        df["espnId"] = df[found].astype(str)

    # Normalize common player name column aliases to `player` (tests use 'name')
    player_aliases = ["player", "name", "player_name", "playerName"]
    player_map = {c.strip(): c for c in df.columns}
    lower_map = {c.strip().lower(): c for c in df.columns}
    player_found = None
    for a in player_aliases:
        if a in player_map:
            player_found = player_map[a]
            break
        if a.lower() in lower_map:
            player_found = lower_map[a.lower()]
            break
    if player_found:
        df["player"] = df[player_found].astype(str)
    else:
        # if no player column, synthesize one from index
        df["player"] = df.index.map(lambda i: f"player_{i}")

    # Ensure a 'week' column exists; default to 0 when missing
    week_aliases = ["week", "game_week", "g", "wk"]
    week_found = None
    for a in week_aliases:
        if a in df.columns:
            week_found = a
            break
    if week_found:
        df["week"] = (
            pd.to_numeric(pd.Series(df[week_found]), errors="coerce")
            .fillna(0)
            .astype(int)
        )
    else:
        df["week"] = 0

    result = compute_stock_qb(df)
    # New weekly_change: position-aware weighted blend of z-scored advanced metrics.
    # sort by player/week for stable output
    result = result.sort_values(["player", "week"])

    # Normalize/ensure a position column exists
    pos_aliases = ["position", "pos", "positionName", "position_name"]
    pos_col = None
    for a in pos_aliases:
        if a in result.columns:
            pos_col = a
            break
    if pos_col:
        result["position"] = result[pos_col].astype(str).fillna("").str.upper()
    else:
        result["position"] = ""

    # Compute z-scores for any numeric-like columns not already z-scored
    for col in list(result.columns):
        zname = f"z_{col}"
        if zname in result.columns:
            continue
        # try to coerce to numeric
        try:
            # result[col] may be typed as Any by static checkers; wrap in Series.
            ser = pd.to_numeric(pd.Series(result[col]), errors="coerce")
        except Exception:
            ser = pd.Series([float("nan")] * len(result), index=result.index)
        if ser.notna().sum() > 0:
            m = ser.mean()
            s = ser.std()
            if s is None or s == 0 or pd.isna(s):
                result[zname] = 0.0
            else:
                result[zname] = ((ser - m) / s).clip(-6.0, 6.0)

    def find_z_series(base_key: str) -> pd.Series:
        """Find a z_ column for a base metric name by exact match or token match.
        Returns a zero series if not found."""
        exact = f"z_{base_key}"
        if exact in result.columns:
            return result[exact].fillna(0.0)
        toks = [t for t in base_key.split("_") if t]
        for c in result.columns:
            if not c.startswith("z_"):
                continue
            name = c[2:].lower()
            if all(tok in name for tok in toks):
                return result[c].fillna(0.0)
        # fallback zero series
        return pd.Series(0.0, index=result.index)

    # Define per-position weights (kept intentionally small)
    weights = {
        "QB": {
            "epa_per_play": 0.05,
            "cpoe": 0.04,
            "yards_per_attempt": 0.03,
            "td_int_ratio": 0.02,
        },
        "RB": {
            "rush_epa": 0.05,
            "yards_after_contact": 0.04,
            "targets_per_game": 0.03,
        },
        "WR": {
            "yards_per_route_run": 0.05,
            "target_share": 0.04,
            "yards_after_catch": 0.03,
        },
        "TE": {
            "yards_per_route_run": 0.05,
            "target_share": 0.04,
            "yards_after_catch": 0.03,
        },
        "DEF": {
            "tackles": 0.03,
            "sacks": 0.04,
            "turnovers_forced": 0.05,
        },
    }

    # Initialize weekly_change series
    weekly_change = pd.Series(0.0, index=result.index)

    # Helper to map position text to our buckets
    def pos_bucket(p: str) -> str:
        if not p:
            return ""
        p = p.upper()
        if p.startswith("QB"):
            return "QB"
        if p.startswith("RB"):
            return "RB"
        if p.startswith("WR"):
            return "WR"
        if p.startswith("TE"):
            return "TE"
        # defensive positions (LB, CB, S, DL, DE, DT, DB)
        if any(
            p.startswith(x) for x in ("LB", "CB", "S", "DL", "DE", "DT", "DB")
        ) or p.startswith("D"):
            return "DEF"
        return ""

    # Vectorized computation per position bucket
    buckets = result["position"].apply(pos_bucket)
    # Heuristic position inference when explicit position is missing
    # Treat any player with pass attempts > 0 as a QB
    if "pass_attempts" in result.columns:
        try:
            pa = pd.to_numeric(
                pd.Series(result["pass_attempts"]), errors="coerce"
            ).fillna(0.0)
            qb_mask = (buckets == "") & (pa > 0)
            buckets.loc[qb_mask] = "QB"
        except Exception:
            pass
    # If targets/target_share exist, infer WR/TE
    if (
        "targets" in result.columns
        or "targets_per_game" in result.columns
        or "target_share" in result.columns
    ):
        try:
            if "targets_per_game" in result.columns:
                tgt_ser = result["targets_per_game"]
            elif "targets" in result.columns:
                tgt_ser = result["targets"]
            else:
                tgt_ser = pd.Series(0.0, index=result.index)
            tgt = pd.to_numeric(pd.Series(tgt_ser), errors="coerce").fillna(0.0)
            wr_mask = (buckets == "") & (tgt > 0)
            buckets.loc[wr_mask] = "WR"
        except Exception:
            pass
    # If rush_yards present and no pass attempts, infer RB
    if "rush_yards" in result.columns:
        try:
            ry = pd.to_numeric(pd.Series(result["rush_yards"]), errors="coerce").fillna(
                0.0
            )
            rb_mask = (buckets == "") & (ry > 0)
            buckets.loc[rb_mask] = "RB"
        except Exception:
            pass
    for bucket, wmap in weights.items():
        mask = buckets == bucket
        if not mask.any():
            continue
        # accumulate weighted z-scores
        for metric, wt in wmap.items():
            zser = find_z_series(metric)
            # elementwise add weight * z
            weekly_change.loc[mask] = weekly_change.loc[mask] + (zser.loc[mask] * wt)

    # Clamp total weekly change to ±0.15 to avoid huge swings
    weekly_change = weekly_change.clip(lower=-0.15, upper=0.15)

    # Assign weekly_change (decimal fraction, e.g., 0.03 = +3%)
    result["weekly_change"] = weekly_change.round(4)

    # Add a percent form for convenience (e.g., 3.2 means +3.2%)
    result["weekly_change_pct"] = (result["weekly_change"] * 100).round(1)

    # Compute new_price when a price-like column exists
    price_aliases = ["price", "currentPrice", "current_price", "price_usd", "price_usd"]
    price_col = None
    for a in price_aliases:
        if a in result.columns:
            price_col = a
            break
    if price_col:
        # Delegate multiplier application to helper so tests can target it directly
        result = apply_volatility_multiplier(result, price_col=price_col)

    summary = summarize_latest(result)

    # Ensure a stable `pass_attempts` column in the summary.
    if "pass_attempts" in result.columns:
        pass_sums = result.groupby("player", as_index=False)["pass_attempts"].sum()
        pass_map = dict(zip(pass_sums["player"], pass_sums["pass_attempts"]))
        summary["pass_attempts"] = (
            summary["player"].map(pass_map).fillna(0).astype(float)
        )
    else:
        summary["pass_attempts"] = pd.NA
        if "z_pass_attempts" in summary.columns:
            summary["pass_attempts_z_fallback"] = (
                summary["z_pass_attempts"].astype(float) >= 0.5
            )
        else:
            print(
                "⚠️ No pass attempt columns found — `pass_attempts` will be NA in summary.",
                file=sys.stderr,
            )

    # Additionally, produce an aggregated per-player CSV that includes
    # fantasy-style stat sums (passing/rushing/receiving) alongside
    # avg EPA/CPOE and play counts. This is written to the same
    # output path so downstream JS can read the richer row shape.

    # Helper to pick a column from several possible aliases
    def pick_col(df, candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    # Map expected input column names to canonical names where possible
    pass_y_col = pick_col(
        df, ["passing_yards", "pass_yards", "pass_yards", "passingYards"]
    )
    pass_td_col = pick_col(
        df, ["passing_tds", "pass_tds", "pass_tds", "passingTDs", "pass_tds"]
    )
    ints_col = pick_col(df, ["interceptions", "ints", "int"])
    rush_y_col = pick_col(
        df, ["rushing_yards", "rush_yards", "rush_yards", "rushingYards"]
    )
    rush_td_col = pick_col(df, ["rushing_tds", "rush_tds", "rush_tds"])
    rec_col = pick_col(df, ["receptions", "rec", "recs", "targets", "targets_per_game"])
    rec_y_col = pick_col(
        df, ["receiving_yards", "receiving_yds", "rec_yards", "receivingYards"]
    )
    rec_td_col = pick_col(df, ["receiving_tds", "rec_tds", "receiving_tds"])
    epa_col = pick_col(df, ["epa_per_play", "epa", "epa/play"])
    cpoe_col = pick_col(df, ["cpoe", "cpoe_pct", "cpoe_percent"])
    play_id_col = pick_col(df, ["play_id", "playid", "playId"])

    # Numeric helper
    def tonum(s):
        try:
            return pd.to_numeric(s, errors="coerce").fillna(0)
        except Exception:
            return pd.Series(0, index=df.index)

    grouped = df.groupby("player")

    agg_df = pd.DataFrame()
    agg_df["player"] = list(grouped.groups.keys())

    # espnId: pick first non-null espnId per player when available
    if "espnId" in df.columns:
        first_espn = grouped["espnId"].first().astype(str)
        agg_df["espnId"] = agg_df["player"].map(lambda p: first_espn.get(p, ""))
    else:
        agg_df["espnId"] = ""

    # avg EPA / avg CPOE
    if epa_col:
        agg_df["avg_epa"] = pd.Series(
            grouped[epa_col].apply(
                lambda s: pd.to_numeric(pd.Series(s), errors="coerce").mean()
            )
        ).fillna(0.0)
    else:
        agg_df["avg_epa"] = 0.0
    if cpoe_col:
        agg_df["avg_cpoe"] = pd.Series(
            grouped[cpoe_col].apply(
                lambda s: pd.to_numeric(pd.Series(s), errors="coerce").mean()
            )
        ).fillna(0.0)
    else:
        agg_df["avg_cpoe"] = 0.0

    # play counts (use play_id when present, otherwise rows per player)
    if play_id_col:
        agg_df["plays"] = (
            pd.Series(
                grouped[play_id_col].apply(
                    lambda s: pd.to_numeric(pd.Series(s), errors="coerce").notna().sum()
                )
            )
            .fillna(0)
            .astype(int)
        )
    else:
        agg_df["plays"] = grouped.size().astype(int).values

    # sums for fantasy stats
    def safe_sum(col):
        if not col:
            return pd.Series(0, index=agg_df.index)
        return grouped[col].apply(
            lambda s: pd.to_numeric(pd.Series(s), errors="coerce").fillna(0).sum()
        )

    # Helper to map a grouped series (indexed by player) back to agg_df order
    def map_series_to_players(series):
        if series is None:
            return pd.Series(0, index=agg_df.index)
        # ensure we have a dict keyed by player name
        try:
            d = series.to_dict()
        except Exception:
            d = {}
        return agg_df["player"].map(lambda p: d.get(p, 0))

    agg_df["passing_yards"] = map_series_to_players(safe_sum(pass_y_col))
    agg_df["passing_tds"] = map_series_to_players(safe_sum(pass_td_col))
    agg_df["interceptions"] = map_series_to_players(safe_sum(ints_col))
    agg_df["rushing_yards"] = map_series_to_players(safe_sum(rush_y_col))
    agg_df["rushing_tds"] = map_series_to_players(safe_sum(rush_td_col))
    agg_df["receptions"] = map_series_to_players(safe_sum(rec_col))
    agg_df["receiving_yards"] = map_series_to_players(safe_sum(rec_y_col))
    agg_df["receiving_tds"] = map_series_to_players(safe_sum(rec_td_col))

    # Aggregate pass_attempts when present (tests expect this column)
    if "pass_attempts" in df.columns:
        pa_series = pd.Series(
            grouped["pass_attempts"].apply(
                lambda s: pd.to_numeric(pd.Series(s), errors="coerce").fillna(0).sum()
            )
        ).fillna(0)
        # map aggregated values back to agg_df order
        agg_df["pass_attempts"] = (
            agg_df["player"].map(lambda p: pa_series.to_dict().get(p, 0)).astype(float)
        )
    else:
        agg_df["pass_attempts"] = 0

    # Preserve position/team where available (take first)
    if "position" in df.columns:
        agg_df["position"] = grouped["position"].first().fillna("")
    else:
        agg_df["position"] = ""
    if "team" in df.columns:
        agg_df["team"] = grouped["team"].first().fillna("")
    else:
        agg_df["team"] = ""

    # Write aggregated CSV to the requested output path
    outp.parent.mkdir(parents=True, exist_ok=True)
    # Ensure deterministic column ordering for downstream consumers
    cols = [
        "player",
        "espnId",
        "position",
        "position_inferred",
        "team",
        "avg_epa",
        "avg_cpoe",
        "plays",
        "pass_attempts",
        "passing_yards",
        "passing_tds",
        "interceptions",
        "rushing_yards",
        "rushing_tds",
        "receptions",
        "receiving_yards",
        "receiving_tds",
    ]
    # Add any missing columns gracefully. Use empty string for text columns.
    text_cols = {"player", "espnId", "position", "position_inferred", "team"}
    for c in cols:
        if c not in agg_df.columns:
            if c in text_cols:
                agg_df[c] = ""
            else:
                agg_df[c] = 0
    agg_df = agg_df[cols]

    # Infer position when it's missing using simple stat-pattern heuristics
    def infer_position(row):
        try:
            pa = float(row.get("pass_attempts", 0) or 0)
        except Exception:
            pa = 0.0
        try:
            ry = float(row.get("rushing_yards", 0) or 0)
        except Exception:
            ry = 0.0
        try:
            rec = float(row.get("receptions", 0) or 0)
        except Exception:
            rec = 0.0
        try:
            rec_y = float(row.get("receiving_yards", 0) or 0)
        except Exception:
            rec_y = 0.0

        if pa >= 20:
            return "QB"
        elif ry >= 50 and rec < 5:
            return "RB"
        elif rec >= 10 and rec_y >= 50:
            return "WR"
        elif rec >= 5 and rec_y < 50:
            return "TE"
        else:
            return "UNK"

    # Populate/overwrite position with inferred position when missing or blank.
    # Be defensive about NaN/None values so we never write the literal string 'nan' or 'NAN'.
    def normalize_pos_val(v):
        if pd.isna(v):
            return ""
        return str(v).strip()

    try:
        # compute an inferred position for every row; infer_position returns 'UNK' when unsure
        agg_df["position_inferred"] = agg_df.apply(
            lambda r: (
                infer_position(r)
                if not normalize_pos_val(r.get("position", ""))
                else normalize_pos_val(r.get("position", ""))
            ),
            axis=1,
        )

        # set the working `position` to existing (normalized) value or inferred
        def pick_final_pos(r):
            existing = normalize_pos_val(r.get("position", ""))
            if existing:
                return existing.upper()
            inferred = r.get("position_inferred", "UNK")
            return (inferred or "UNK").upper()

        agg_df["position"] = agg_df.apply(pick_final_pos, axis=1)
    except Exception:
        # fallback: ensure columns exist and have sane defaults
        if "position" not in agg_df.columns:
            agg_df["position"] = "UNK"
        if "position_inferred" not in agg_df.columns:
            agg_df["position_inferred"] = agg_df["position"]

    # Try to overwrite UNK positions using cleaned profiles when available
    profiles_path = Path("data/player_profiles_cleaned.csv")
    # prepare a `position_profile` column to capture the raw profile value when used
    agg_df["position_profile"] = ""

    if profiles_path.exists():
        try:
            prof_df = pd.read_csv(profiles_path)
            # detect name and espn id columns
            name_col = None
            id_col = None
            for c in prof_df.columns:
                lc = c.lower()
                if lc in ("player", "player_name", "name") and name_col is None:
                    name_col = c
                if (
                    lc in ("espnid", "espn_id", "espn", "playerid", "player_id", "id")
                    and id_col is None
                ):
                    id_col = c

            name_map = {}
            id_map = {}
            # position field may be 'position', 'pos', or 'position_name'
            pos_col = None
            for c in prof_df.columns:
                if c.lower() in ("position", "pos", "position_name"):
                    pos_col = c
                    break

            if pos_col:
                for _, prow in prof_df.iterrows():
                    pname = str(prow.get(name_col, "")).strip() if name_col else ""
                    pid = str(prow.get(id_col, "")).strip() if id_col else ""
                    raw_ppos = prow.get(pos_col, "")
                    # Normalize profile position; skip empty/null/NaN profile positions
                    ppos = ""
                    if raw_ppos is not None and not pd.isna(raw_ppos):
                        ppos = str(raw_ppos).strip().upper()
                    # Only record non-empty profile positions
                    if ppos:
                        if pname:
                            name_map[pname.lower()] = ppos
                        if pid:
                            id_map[pid] = ppos

            # apply profile overwrite when the current position is missing/UNK
            # Vectorized approach: map espnId and player name to profile positions,
            # then only apply where current position is empty or 'UNK'. This avoids
            # per-row .loc/.iat and is friendlier to static type checkers.
            agg_df["position_overwritten_from_profile"] = False

            # Map espnId -> profile pos and player name -> profile pos
            esp_map_series = (
                agg_df["espnId"].astype(str).map(id_map).fillna("")
                if "espnId" in agg_df.columns
                else pd.Series("", index=agg_df.index)
            )
            name_map_series = (
                agg_df["player"]
                .astype(str)
                .str.strip()
                .str.lower()
                .map(name_map)
                .fillna("")
            )

            # candidate position prefers espnId mapping, falls back to name mapping
            candidate = esp_map_series.where(
                esp_map_series != "", name_map_series
            ).fillna("")

            # rows where position is missing or 'UNK'
            cur_pos_series = (
                agg_df["position"].astype(str).fillna("").str.strip().str.upper()
            )
            missing_mask = (cur_pos_series == "") | (cur_pos_series == "UNK")

            # decide which rows to overwrite: missing AND candidate non-empty
            use_mask = missing_mask & (candidate != "")

            if use_mask.any():
                # write final values (uppercase) and mark provenance
                agg_df.loc[use_mask, "position"] = candidate[use_mask].str.upper()
                agg_df.loc[use_mask, "position_profile"] = candidate[
                    use_mask
                ].str.upper()
                agg_df.loc[use_mask, "position_overwritten_from_profile"] = True
        except Exception:
            # if profiles parsing fails, continue without overwriting
            pass

    # Apply QB minimum pass-attempts filter: require QBs to have at least 20 pass attempts
    try:
        if "pass_attempts" in agg_df.columns and "position" in agg_df.columns:
            agg_df = agg_df[
                (agg_df["pass_attempts"] >= 20) | (agg_df["position"] != "QB")
            ]
    except Exception:
        # If filtering fails for any reason, fall back to unfiltered DataFrame
        pass

    agg_df.to_csv(outp, index=False)

    history_out = outp.parent / "player_stock_history.csv"
    zcols_all = [c for c in result.columns if c.startswith("z_")]

    # detect a date column to use for timestamps (nflfastR/game-level date if available)
    date_col = None
    for c in result.columns:
        lc = c.lower()
        if lc in ("game_date", "date", "game_date_est", "gamedate", "timestamp"):
            date_col = c
            break

    if date_col:
        try:
            # Ensure we pass a Series to pd.to_datetime so type checkers
            # and pandas both handle mixed dtypes consistently.
            # mypy has difficulty reasoning about the mixed dtypes that may appear in
            # this DataFrame column; coerce with pandas and accept the runtime result.
            _series = pd.Series(result[date_col])
            result["__game_date"] = pd.to_datetime(_series, errors="coerce")  # type: ignore[assignment]
        except Exception:
            result["__game_date"] = pd.NaT
    else:
        result["__game_date"] = pd.NaT

    # Build a per-player daily timeline using per-game `stock` as the post-game price
    timeline_rows: list[dict[str, Any]] = []
    grouped_players: Any = result.sort_values(
        ["player", "week", "__game_date"]
    ).groupby("player")
    for pname, group in grouped_players:
        # ensure deterministic order: by game_date first then week
        group_sorted = group.sort_values(["__game_date", "week"]).reset_index(drop=True)
        prev_date = None
        prev_price = None
        for i, (_, row) in enumerate(group_sorted.iterrows()):
            # Prefer the parsed game date (__game_date) when available
            gdate: Any = row.get("__game_date")
            # Replace missing/invalid or obviously-bad dates (e.g., 1970 epoch) with 2025 season dates.
            replace_date = False
            try:
                if pd.isna(gdate):
                    replace_date = True
                else:
                    # If timestamp exists but is before 2025, treat it as invalid for our purposes
                    if isinstance(gdate, pd.Timestamp) and (gdate.year < 2025):
                        replace_date = True
            except Exception:
                replace_date = True

            if replace_date:
                # Determine a week number if present and usable
                wk = None
                try:
                    raw_wk = row.get("week", None)
                    if raw_wk is not None and raw_wk != "":
                        wk_val = _safe_int(raw_wk, default=0)
                        if wk_val > 0:
                            wk = wk_val
                except Exception:
                    wk = None

                # NFL 2025 season Week 1 baseline
                base_week1 = pd.Timestamp("2025-09-05")
                if wk and wk >= 1:
                    # map Week N -> base + (N-1) weeks
                    try:
                        # use _safe_int to ensure a proper integer for the weeks param
                        wk_offset = _safe_int(wk, default=1) - 1
                        gdate = base_week1 + pd.Timedelta(weeks=wk_offset)
                    except Exception:
                        gdate = base_week1 + pd.Timedelta(weeks=_safe_int(i, default=0))
                else:
                    # no valid week: assign sequential weekly dates per-player starting at Week 1
                    try:
                        gdate = base_week1 + pd.Timedelta(weeks=_safe_int(i, default=0))
                    except Exception:
                        gdate = pd.NaT

            # If gdate is a string or other type, try to parse it into a Timestamp.
            try:
                if not isinstance(gdate, pd.Timestamp):
                    parsed = pd.to_datetime(pd.Series([gdate]), errors="coerce").iloc[0]
                    if not pd.isna(parsed):
                        gdate = parsed
            except Exception:
                pass

            # If we still don't have a valid gdate, synthesize one deterministically
            if pd.isna(gdate):
                # prefer week field when available
                try:
                    raw_wk2 = row.get("week", None)
                    wk2 = (
                        _safe_int(raw_wk2, default=0)
                        if raw_wk2 is not None and raw_wk2 != ""
                        else 0
                    )
                except Exception:
                    wk2 = 0
                if wk2 and wk2 >= 1:
                    gdate = base_week1 + pd.Timedelta(weeks=(wk2 - 1))
                else:
                    # fallback: sequential per-player week using enumerated index
                    gdate = base_week1 + pd.Timedelta(weeks=_safe_int(i, default=0))

            # Safely coerce stock to numeric (avoid passing Any|None to float())
            raw_price = row.get("stock") if "stock" in row else None
            try:
                parsed_price = pd.to_numeric(
                    pd.Series([raw_price]), errors="coerce"
                ).iloc[0]
                price = float(parsed_price) if not pd.isna(parsed_price) else 0.0
            except Exception:
                price = 0.0

            ts = ""
            if not pd.isna(gdate):
                # Ensure ISO date string (YYYY-MM-DD)
                try:
                    ts = pd.Timestamp(gdate).strftime("%Y-%m-%d")
                except Exception:
                    ts = ""

            # Safely coerce optional values to expected scalar types. If week was missing/0, prefer the derived week
            week_val = row.get("week", None) if "week" in row else None
            try:
                if week_val is None or pd.isna(week_val) or int(week_val) == 0:
                    # if original week missing, derive from synthetic date if available
                    if not pd.isna(gdate):
                        # compute week number relative to base_week1
                        try:
                            base_week1 = pd.Timestamp("2025-09-05")
                            delta_days = (
                                pd.Timestamp(gdate).normalize() - base_week1.normalize()
                            ).days
                            derived_week = (
                                max(1, int(math.floor(delta_days / 7.0)) + 1)
                                if delta_days is not None
                                else 0
                            )
                            week_field = int(derived_week)
                        except Exception:
                            week_field = 0
                    else:
                        week_field = 0
                else:
                    week_field = _safe_int(week_val, default=0)
            except Exception:
                week_field = 0
            entry = {
                "player": pname,
                "timestamp": ts,
                "price": round(price, 4),
                "week": week_field,
                "stock": round(price, 4),
                "confidence": _safe_float_maybe(row.get("C")) if "C" in row else "",
                "weekly_change": (
                    _safe_float_maybe(row.get("weekly_change"))
                    if "weekly_change" in row
                    else ""
                ),
                "weekly_change_pct": (
                    _safe_float_maybe(row.get("weekly_change_pct"))
                    if "weekly_change_pct" in row
                    else ""
                ),
                "rawChange": (
                    _safe_float_maybe(row.get("rawChange"))
                    if "rawChange" in row
                    else ""
                ),
                "multiplier": (
                    _safe_float_maybe(row.get("multiplier"))
                    if "multiplier" in row
                    else ""
                ),
                "cappedChange": (
                    _safe_float_maybe(row.get("cappedChange"))
                    if "cappedChange" in row
                    else ""
                ),
                "newPrice": (
                    _safe_float_maybe(row.get("newPrice")) if "newPrice" in row else ""
                ),
                "espnId": row.get("espnId", "") if "espnId" in row else "",
            }
            for z in zcols_all:
                entry[z] = row.get(z, "")

            timeline_rows.append(entry)

            # interpolate daily points between prev_date and current game date
            if (
                prev_date is not None
                and not pd.isna(prev_date)
                and not pd.isna(gdate)
                and prev_price is not None
            ):
                days = int((gdate.normalize() - prev_date.normalize()).days)
                if days > 1:
                    for d in range(1, days):
                        interp_date = (
                            prev_date + pd.Timedelta(days=_safe_int(d, default=0))
                        ).strftime("%Y-%m-%d")
                        frac = d / float(days)
                        interp_price = prev_price + (price - prev_price) * frac
                        timeline_rows.append(
                            {
                                "player": pname,
                                "timestamp": interp_date,
                                "price": round(float(interp_price), 4),
                                "week": "",
                                "stock": "",
                                "confidence": "",
                                "weekly_change": "",
                                "weekly_change_pct": "",
                                "espnId": (
                                    row.get("espnId", "") if "espnId" in row else ""
                                ),
                            }
                        )

            prev_date = gdate if not pd.isna(gdate) else prev_date
            prev_price = price

    hist_out_df = pd.DataFrame(timeline_rows)
    # deterministic column ordering
    hist_cols_out = [
        "player",
        "timestamp",
        "price",
        "week",
        "stock",
        "confidence",
        "weekly_change",
        "weekly_change_pct",
        "rawChange",
        "multiplier",
        "cappedChange",
        "newPrice",
        "espnId",
    ] + zcols_all
    for c in hist_cols_out:
        if c not in hist_out_df.columns:
            hist_out_df[c] = ""
    hist_out_df = hist_out_df[hist_cols_out]
    hist_out_df = hist_out_df.sort_values(["player", "timestamp"])
    hist_out_df.to_csv(history_out, index=False)

    # Print a small sanity message for CLI users
    print(
        f"Processed {len(summary)} players — summary written to {outp} and history written to {history_out}"
    )
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Compute player stock from per-game stats CSV (QB formula)"
    )
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        default="data/player_game_stats.csv",
        help="Input CSV path",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="data/player_stock_summary.csv",
        help="Output CSV path",
    )
    args = parser.parse_args(argv)
    inp = Path(args.input)
    outp = Path(args.output)
    if not inp.exists():
        print(
            f"Input file {inp} not found. Please provide a CSV with columns: "
            "player, week, pass_yards, pass_tds, ints, rush_yards, "
            "rush_tds, fumbles, epa_per_play, cpoe, pass_attempts",
            file=sys.stderr,
        )
        sys.exit(2)
    compute_player_stock_summary(str(inp), str(outp))


if __name__ == "__main__":
    main()
