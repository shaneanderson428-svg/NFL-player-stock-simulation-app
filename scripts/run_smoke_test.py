from pathlib import Path
import pandas as pd
import pandas.testing as pdt


# Local test parameters (match notebook defaults)
year = 2025
needed_cols = ["play_id", "passer_player_name", "epa", "cpoe"]
dtypes = {
    "play_id": "Int64",
    "passer_player_name": "string",
    "epa": "float32",
    "cpoe": "float32",
}
chunksize = 2

out_dir = Path("data")
out_dir.mkdir(parents=True, exist_ok=True)
sample_path = out_dir / f"play_by_play_{year}_sample.csv.gz"

# Create sample
sample = pd.DataFrame(
    [
        {"play_id": 1, "passer_player_name": "A Q", "epa": 0.5, "cpoe": 0.1},
        {"play_id": 2, "passer_player_name": "A Q", "epa": 1.0, "cpoe": 0.2},
        {"play_id": 3, "passer_player_name": "B Q", "epa": -0.2, "cpoe": -0.1},
        {"play_id": 4, "passer_player_name": None, "epa": 0.0, "cpoe": 0.0},
    ]
)

sample.to_csv(sample_path, index=False, compression="gzip")

# Non-chunked summary
try:
    df_nc = pd.read_csv(str(sample_path), compression="gzip", usecols=needed_cols)
except Exception:
    # fallback
    df_nc = pd.read_csv(str(sample_path), compression="gzip", low_memory=False)

summary_nc = (
    df_nc.groupby("passer_player_name", dropna=True)
    .agg(avg_epa=("epa", "mean"), avg_cpoe=("cpoe", "mean"), plays=("play_id", "count"))
    .loc[lambda d: d["plays"] >= 1]
    .sort_values("avg_epa", ascending=False)
)

# Chunked summary
acc: dict[str, list[float]] = {}
for chunk in pd.read_csv(
    str(sample_path),
    compression="gzip",
    usecols=needed_cols,
    chunksize=max(1, int(chunksize)),
):
    chunk = chunk.dropna(subset=["passer_player_name"])
    grp = chunk.groupby("passer_player_name", dropna=True).agg(
        sum_epa=("epa", "sum"), sum_cpoe=("cpoe", "sum"), plays=("play_id", "count")
    )
    for name, row in grp.iterrows():
        if name in acc:
            acc[name][0] += float(row["sum_epa"])
            acc[name][1] += float(row["sum_cpoe"])
            acc[name][2] += int(row["plays"])
        else:
            acc[name] = [
                float(row["sum_epa"]),
                float(row["sum_cpoe"]),
                int(row["plays"]),
            ]

if acc:
    agg_df = pd.DataFrame.from_dict(
        acc, orient="index", columns=["sum_epa", "sum_cpoe", "plays"]
    )
    agg_df.index.name = "passer_player_name"
    agg_df = agg_df.reset_index()
    agg_df["avg_epa"] = agg_df["sum_epa"] / agg_df["plays"]
    agg_df["avg_cpoe"] = agg_df["sum_cpoe"] / agg_df["plays"]
    summary_ch = (
        agg_df.loc[lambda d: d["plays"] >= 1]
        .sort_values("avg_epa", ascending=False)
        .loc[:, ["passer_player_name", "avg_epa", "avg_cpoe", "plays"]]
    )
    summary_ch = summary_ch.set_index("passer_player_name")
else:
    summary_ch = pd.DataFrame(columns=["avg_epa", "avg_cpoe", "plays"])

# Compare results (rounded and normalized)

nc = summary_nc.round(6).sort_index()
ch = summary_ch.round(6).sort_index()
# Normalize index to strings to avoid dtype mismatches
nc.index = nc.index.astype(str)
ch.index = ch.index.astype(str)

try:
    pdt.assert_frame_equal(nc, ch, check_dtype=False)
    print("SMOKE TEST: MATCH — chunked and non-chunked results are equal")
    print("\nSummary:")
    print(nc)
    result = True
except AssertionError as e:
    print("SMOKE TEST: MISMATCH —", e)
    print("\nNon-chunked summary:")
    print(nc)
    print("\nChunked summary:")
    print(ch)
    result = False

if not result:
    raise SystemExit(2)
else:
    raise SystemExit(0)
