import pandas as pd

fn = "data/external/play_by_play_demo.csv"
df = pd.read_csv(fn)
s = (
    df.groupby("passer_player_name")[["epa", "cpoe"]]
    .mean()
    .reset_index()
    .sort_values("epa", ascending=False)
)
print("\nTop 10 passers by average EPA (demo data):")
print(s.head(10).to_string(index=False))
s.to_csv("epa_cpoe_summary_demo.csv", index=False)
print("\nSaved demo summary to epa_cpoe_summary_demo.csv")
