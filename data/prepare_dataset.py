import pandas as pd

# Load raw OPSD data
df = pd.read_csv(
    "time_series_60min_singleindex.csv",
    parse_dates=["utc_timestamp"]
)

# Select ONE real signal (Austria load)
df = df[["utc_timestamp", "AT_load_actual_entsoe_transparency"]].dropna()

# Rename cleanly
df.rename(columns={
    "utc_timestamp": "timestamp",
    "AT_load_actual_entsoe_transparency": "load_mw"
}, inplace=True)

# Normalize load to 0–100 scale (stress baseline)
max_load = df["load_mw"].max()
df["load_normalized"] = (df["load_mw"] / max_load) * 100

# Save cleaned dataset
df.to_csv("data/grid_load_clean.csv", index=False)

print("Dataset prepared: data/grid_load_clean.csv")

