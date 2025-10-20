#!/usr/bin/env python3
import os, re, sys
import pandas as pd

IN = sys.argv[1] if len(sys.argv) > 1 else "/home/tahmast/Projects/Tourism/Dataset/airports.csv"
OUT = "data/curated/airports_it.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)

# Important: dtype=str and keep_default_na=False to preserve values like "00A"
df = pd.read_csv(IN, dtype=str, keep_default_na=False, low_memory=False)

# Select/rename columns (if names differ, expand the 'alt' mapping below)
colmap = {
    "ident": "ident", "type": "type", "name": "name",
    "latitude_deg": "lat", "longitude_deg": "lon",
    "gps_code": "icao_code", "iata_code": "airport_code",
    "municipality": "city_name", "iso_region": "region_code", "iso_country": "iso_country"
}
alt = {"latitude_deg": ["latitude", "lat_deg"], "longitude_deg": ["longitude", "lon_deg"], "iata_code": ["iata"]}

use = {}
for k, v in colmap.items():
    if k in df.columns:
        use[k] = v
    else:
        for a in alt.get(k, []):
            if a in df.columns:
                use[a] = v
                break

if "iso_country" not in use.values():
    raise SystemExit("✖ iso_country column not found in input CSV.")

df = df[list(use.keys())].rename(columns=use)

# Convert coordinates to numeric
df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

# Filter only Italian airports + valid 3-letter IATA + valid coordinates
it = df[
    (df["iso_country"] == "IT") &
    (df["airport_code"].str.fullmatch(r"[A-Z]{3}", na=False)) &
    (df["lat"].between(-90, 90)) &
    (df["lon"].between(-180, 180))
].copy()

# For duplicate IATA codes: keep large > medium > small
rank = {"large_airport": 3, "medium_airport": 2, "small_airport": 1}
it["type_rank"] = it["type"].map(rank).fillna(0)
it = (
    it.sort_values(["airport_code", "type_rank"], ascending=[True, False])
      .drop_duplicates("airport_code", keep="first")
      .drop(columns=["type_rank"])
)

# Final column order
cols = ["airport_code", "icao_code", "name", "type", "lat", "lon", "city_name", "region_code", "ident"]
for c in cols:
    if c not in it.columns:
        it[c] = ""
it = it[cols].reset_index(drop=True)

it.to_csv(OUT, index=False, encoding="utf-8")
print(f"✓ saved: {OUT} | rows={len(it)}")
print(it.head(10).to_string(index=False))
