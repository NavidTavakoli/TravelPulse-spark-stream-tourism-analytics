#!/usr/bin/env python3


import os
import re
import argparse
import unicodedata
import numpy as np
import pandas as pd

# ---------- Utils ----------
def normalize_text_series(s: pd.Series) -> pd.Series:
    def _norm(x):
        if pd.isna(x):
            return ""
        x = str(x)
        x = unicodedata.normalize("NFKD", x)
        x = "".join(ch for ch in x if not unicodedata.combining(ch))
        x = x.lower().strip()
        x = x.replace("’", "'")
        for ch in [",",";","'","\"", "(",")",".","-","_","/","\\"]:
            x = x.replace(ch, " ")
        x = " ".join(x.split())
        return x
    return s.map(_norm)

def read_csv_any(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def extract_year_any(x) -> float:
    if pd.isna(x): return np.nan
    m = re.search(r"(20\d{2}|19\d{2})", str(x))
    return float(m.group(1)) if m else np.nan

def prep_istat(df: pd.DataFrame,
               name_candidates=("territory_name","territory","comune","name"),
               code_candidates=("territory_code","code"),
               year_candidates=("year","time","TIME")):
    d = df.copy()
    low_map = {c.lower(): c for c in d.columns}

    name_col = next((low_map[c.lower()] for c in name_candidates if c.lower() in low_map), None)
    if name_col is None:
        for c in d.columns:
            cl = c.lower()
            if "territ" in cl or "comune" in cl or "region" in cl or "name" in cl:
                name_col = c; break

    code_col = next((low_map[c.lower()] for c in code_candidates if c.lower() in low_map), None)
    year_col = next((low_map[c.lower()] for c in year_candidates if c.lower() in low_map), None)

    d["__key_norm"] = normalize_text_series(d[name_col]) if name_col else ""

    if year_col and year_col in d.columns:
        d["_year_extracted"] = d[year_col].apply(extract_year_any)
        d = d.sort_values("_year_extracted").groupby("__key_norm", as_index=False).tail(1)
        d = d.drop(columns=[c for c in ["_year_extracted"] if c in d.columns], errors="ignore")
    return d

def select_cols(df: pd.DataFrame, wanted_lower: list) -> pd.DataFrame:
    lmap = {c.lower(): c for c in df.columns}
    cols = [lmap[w] for w in wanted_lower if w in lmap]
    cols = list(dict.fromkeys(cols + ["__key_norm"]))
    return df[cols].copy()

def safe_merge(left: pd.DataFrame, right: pd.DataFrame, suffix: str) -> pd.DataFrame:
    common = set(left.columns).intersection(set(right.columns)) - {"__key_norm"}
    right_renamed = right.rename(columns={c: f"{c}{suffix}" for c in common})
    return left.merge(right_renamed, on="__key_norm", how="left", suffixes=("", suffix))

# ---------- Main ----------
def run(
    cities_path: str,
    households_path: str,
    homes_path: str,
    families_path: str,
    commuting_path: str,
    out_csv: str,
    report_csv: str
):
    # Load
    cities = read_csv_any(cities_path)
    households = read_csv_any(households_path)
    homes = read_csv_any(homes_path)
    families = read_csv_any(families_path)
    commuting = read_csv_any(commuting_path)

    # Detect/rename city core cols
    cmap = {c.lower(): c for c in cities.columns}
    city_name_col = cmap.get("city_name") or cmap.get("name") or list(cities.columns)[1]
    region_col = cmap.get("region") or cmap.get("admin1_code") or None
    lat_col = cmap.get("lat") or "lat"
    lon_col = cmap.get("lon") or "lon"
    pop_col = cmap.get("population") or "population"
    city_id_col = cmap.get("city_id") or cmap.get("id") or None

    cities["__key_norm"] = normalize_text_series(cities[city_name_col])

    # Prepare ISTAT tables
    households_p = prep_istat(households)
    homes_p = prep_istat(homes)
    families_p = prep_istat(families)
    commuting_p = prep_istat(commuting)

    # Select relevant cols
    households_sel = select_cols(households_p, ["territory_name","households","year","territory_code"])
    homes_sel      = select_cols(homes_p,      ["territory_name","dwellings_total","dwellings_occupied","occupied_share","year"])
    families_sel   = select_cols(families_p,   ["territory_name","families_total","families_3plus_share","avg_family_size_from_istat","year"])
    commuting_sel  = select_cols(commuting_p,  ["territory_name","resident_population","commuting_population","commuting_ratio","year"])

    # Merge
    enriched = cities.copy()
    enriched = safe_merge(enriched, households_sel, "_hh")
    enriched = safe_merge(enriched, homes_sel, "_home")
    enriched = safe_merge(enriched, families_sel, "_fam")
    enriched = safe_merge(enriched, commuting_sel, "_com")

    # Standardize names
    rename_map = {
        city_name_col: "city_name",
        (region_col or "region"): "region",
        (lat_col or "lat"): "lat",
        (lon_col or "lon"): "lon",
        (pop_col or "population"): "population",
    }
    if city_id_col:
        rename_map[city_id_col] = "city_id"
    enriched = enriched.rename(columns=rename_map)

    if "city_id" not in enriched.columns:
        enriched.insert(0, "city_id", range(1, len(enriched)+1))

    # Normalize derived column names (from suffixed merges)
    norm_renames = {}
    for c in enriched.columns:
        cl = c.lower()
        if cl == "resident_population_com": norm_renames[c] = "resident_population"
        if cl == "commuting_population_com": norm_renames[c] = "commuting_population"
        if cl == "commuting_ratio_com": norm_renames[c] = "commuting_ratio"
        if cl == "dwellings_total_home": norm_renames[c] = "dwellings_total"
        if cl == "dwellings_occupied_home": norm_renames[c] = "dwellings_occupied"
        if cl == "occupied_share_home": norm_renames[c] = "occupied_share"
        if cl == "families_total_fam": norm_renames[c] = "families_total"
        if cl == "families_3plus_share_fam": norm_renames[c] = "families_3plus_share"
        if cl == "avg_family_size_from_istat_fam": norm_renames[c] = "avg_family_size_from_istat"
    enriched = enriched.rename(columns=norm_renames)

    # avg_family_size (fallback = population / households)
    hh_col = next((c for c in enriched.columns if c.lower() == "households"), None)
    if "avg_family_size_from_istat" in enriched.columns:
        enriched["avg_family_size"] = enriched["avg_family_size_from_istat"]
        if hh_col:
            fallback = pd.to_numeric(enriched["population"], errors="coerce") / pd.to_numeric(enriched[hh_col], errors="coerce").replace({0: np.nan})
            enriched["avg_family_size"] = enriched["avg_family_size"].fillna(fallback)
    else:
        if hh_col:
            enriched["avg_family_size"] = pd.to_numeric(enriched["population"], errors="coerce") / pd.to_numeric(enriched[hh_col], errors="coerce").replace({0: np.nan})
        else:
            enriched["avg_family_size"] = np.nan

    # Order columns
    preferred = [
        "city_id","city_name","region","lat","lon","population",
        "households","avg_family_size",
        "dwellings_total","dwellings_occupied","occupied_share",
        "families_total","families_3plus_share",
        "resident_population","commuting_population","commuting_ratio"
    ]
    final_cols = [c for c in preferred if c in enriched.columns]
    final_cols += [c for c in enriched.columns if c not in final_cols and not c.startswith("__")]

    enriched_out = enriched[final_cols].copy()

    # Unmatched report (most missing)
    missing_candidates = [c for c in ["households","dwellings_total","families_total","resident_population","commuting_ratio"] if c in enriched_out.columns]
    if missing_candidates:
        missing_score = enriched_out[missing_candidates].isna().sum(axis=1)
    else:
        missing_score = enriched_out.isna().sum(axis=1)
    report = enriched_out.assign(__missing_count=missing_score)\
                         .sort_values(["__missing_count","population"], ascending=[False, False])\
                         .loc[:, ["city_id","city_name","region","population","__missing_count"]]\
                         .head(500)

    # Save
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    os.makedirs(os.path.dirname(report_csv), exist_ok=True)
    enriched_out.to_csv(out_csv, index=False)
    report.to_csv(report_csv, index=False)

    print(f"[OK] Enriched written: {out_csv}")
    print(f"[OK] Unmatched report: {report_csv}")
    print(f"[INFO] Rows: {len(enriched_out):,} | Columns: {len(enriched_out.columns):,}")

def parse_args():
    p = argparse.ArgumentParser(description="Merge Italian reference CSVs into an enriched city file.")
    p.add_argument("--cities",    default="cities_it.csv")
    p.add_argument("--households",default="istat_households.csv")
    p.add_argument("--homes",     default="istat_homes.csv")
    p.add_argument("--families",  default="istat_families.csv")
    p.add_argument("--commuting", default="istat_commuting.csv")
    p.add_argument("--out",       default=os.path.join("data","curated","cities_it_enriched_full.csv"))
    p.add_argument("--report",    default=os.path.join("data","curated","reports","unmatched_top500.csv"))
    return p.parse_args()

if __name__ == "__main__":

    BASE = "/home/tahmast/Projects/Tourism/data/curated"

    cities_path     = f"{BASE}/cities_it.csv"
    households_path = f"{BASE}/istat_households.csv"
    homes_path      = f"{BASE}/istat_homes.csv"
    families_path   = f"{BASE}/istat_families.csv"
    commuting_path  = f"{BASE}/istat_commuting.csv"

    out_csv    = f"{BASE}/cities_it_enriched_full.csv"
    report_csv = f"{BASE}/reports/unmatched_top500.csv"

    run(
        cities_path=cities_path,
        households_path=households_path,
        homes_path=homes_path,
        families_path=families_path,
        commuting_path=commuting_path,
        out_csv=out_csv,
        report_csv=report_csv,
    )
