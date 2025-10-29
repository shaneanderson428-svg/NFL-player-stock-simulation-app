import pandas as pd

url = (
    "https://github.com/nflverse/nflfastR-data/raw/master/data/play_by_play_2024.csv.gz"
)
print("Attempting to read sample rows from:", url)
try:
    # limit rows to keep this quick and robust in CI-like envs
    df = pd.read_csv(url, compression="gzip", low_memory=False, nrows=200000)
    print(f"Read {len(df):,} rows (sample)")
    s = (
        df.groupby("passer_player_name")[["epa", "cpoe"]]
        .mean()
        .reset_index()
        .dropna(subset=["passer_player_name"])
        .sort_values("epa", ascending=False)
    )
    print("\nTop 20 passers by average EPA (sample):")
    print(s.head(20).to_string(index=False))
    s.to_csv("epa_cpoe_summary_2024_sample.csv", index=False)
    print("\nSaved sample summary to epa_cpoe_summary_2024_sample.csv")
except Exception as e:
    print("Error reading play-by-play CSV:", type(e).__name__, str(e))
    raise
