import pandas as pd

df = pd.read_csv(
    "time_series_60min_singleindex.csv",
    parse_dates=["utc_timestamp"]
)

# Pick one region (keep it simple)
df = df[["utc_timestamp", "AT_load_actual_entsoe_transparency"]].dropna()

df.rename(columns={
    "utc_timestamp": "timestamp",
    "AT_load_actual_entsoe_transparency": "load_mw"
}, inplace=True)

# Normalize load → loading %
df["loading_pct"] = (df["load_mw"] / df["load_mw"].max()) * 100

df.to_csv("data/opsd_clean.csv", index=False)
print("Saved cleaned dataset")

