#!/usr/bin/env python3

import argparse
import pandas as pd
import numpy as np
import unicodedata
from pathlib import Path

def norm_key_series(s: pd.Series) -> pd.Series:
    """Normalize a text series for robust name-based joins (lowercase, strip accents, collapse spaces)."""
    def _n(x):
        if pd.isna(x):
            return np.nan
        x = str(x).strip().lower()
        x = "".join(ch for ch in unicodedata.normalize("NFKD", x) if not unicodedata.combining(ch))
        # Replace punctuation with spaces, unify separators
        for ch in [".", ",", ";", ":", "'", '"', "’", "‘", "´", "`", "(", ")", "[", "]", "{", "}", "/"]:
            x = x.replace(ch, " ")
        x = x.replace("-", " ")
        x = " ".join(x.split())
        return x
    return s.map(_n)

def read_csv(path: Path, dtype=str) -> pd.DataFrame:

    last_err = None
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        try:
            return pd.read_csv(path, dtype=dtype, encoding=enc)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Failed to read {path}: {last_err}")

def prep_cities_base(cities_path: Path) -> pd.DataFrame:
    df = read_csv(cities_path, dtype=str)

    if "__name_norm" in df.columns:
        df["__key"] = norm_key_series(df["__name_norm"])
    elif "asciiname" in df.columns:
        df["__key"] = norm_key_series(df["asciiname"])
    elif "name_norm" in df.columns:
        df["__key"] = norm_key_series(df["name_norm"])
    else:

        name_col = "city_name" if "city_name" in df.columns else df.columns[0]
        df["__key"] = norm_key_series(df[name_col])
    return df

def prep_istat(df: pd.DataFrame) -> pd.DataFrame:
    """Keep municipal-level rows only and build normalized name key. Drop names that are ambiguous in that table."""
    df = df.copy()
    df["territory_code"] = df["territory_code"].astype(str)
    # Municipal ISTAT codes are 6-digit numeric
    is_municipal = df["territory_code"].str.fullmatch(r"\d{6}")
    df = df[is_municipal].copy()
    # Name key
    df["__key"] = norm_key_series(df["territory_name"])
    vc = df["__key"].value_counts(dropna=False)
    unique_names = set(vc[vc == 1].index)
    df = df[df["__key"].isin(unique_names)].copy()
    return df

def merge_all(cities: pd.DataFrame,
              households: pd.DataFrame,
              homes: pd.DataFrame,
              families: pd.DataFrame,
              commuting: pd.DataFrame) -> pd.DataFrame:
    def sel(df: pd.DataFrame, keep_cols):
        cols = ["territory_code", "territory_name", "__key"] + [c for c in keep_cols if c in df.columns]
        return df.loc[:, [c for c in cols if c in df.columns]].copy()

    households_p = sel(households, ["households", "year"])
    homes_p      = sel(homes, ["dwellings_total", "dwellings_occupied", "occupied_share"])
    families_p   = sel(families, ["families_total", "families_3plus_share", "avg_family_size_from_istat"])
    commuting_p  = sel(commuting, ["resident_population", "commuting_population", "commuting_ratio"])

    if "year" in households_p.columns:
        households_p["__year_num"] = pd.to_numeric(households_p["year"], errors="coerce")
        households_p = households_p.sort_values(["__key", "__year_num"], ascending=[True, False]) \
                                   .drop_duplicates("__key") \
                                   .drop(columns="__year_num")

    enriched = cities.merge(households_p.drop(columns=["territory_name"]), how="left", on="__key", suffixes=("", "_households"))
    enriched = enriched.merge(homes_p.drop(columns=["territory_name"]),      how="left", on="__key", suffixes=("", "_homes"))
    enriched = enriched.merge(families_p.drop(columns=["territory_name"]),   how="left", on="__key", suffixes=("", "_families"))
    enriched = enriched.merge(commuting_p.drop(columns=["territory_name"]),  how="left", on="__key", suffixes=("", "_commuting"))

    istat_candidates = [c for c in enriched.columns if c.startswith("territory_code_")] + \
                       (["territory_code"] if "territory_code" in enriched.columns else [])
    if istat_candidates:
        enriched["istat_comune_code"] = np.nan
        for col in istat_candidates:
            enriched["istat_comune_code"] = enriched["istat_comune_code"].fillna(enriched[col])
        enriched = enriched.drop(columns=istat_candidates, errors="ignore")

    cols = [c for c in enriched.columns if c != "__key"] + ["__key"]
    enriched = enriched[cols]

    return enriched

def main():
    ap = argparse.ArgumentParser(description="Merge ISTAT reference CSVs into cities_it.csv")
    ap.add_argument("--cities", default="cities_it.csv", help="Path to cities_it.csv")
    ap.add_argument("--households", default="istat_households.csv", help="Path to istat_households.csv")
    ap.add_argument("--homes", default="istat_homes.csv", help="Path to istat_homes.csv")
    ap.add_argument("--families", default="istat_families.csv", help="Path to istat_families.csv")
    ap.add_argument("--commuting", default="istat_commuting.csv", help="Path to istat_commuting.csv")
    ap.add_argument("--out", default="cities_it_enriched_full.csv", help="Output CSV path")
    args = ap.parse_args()

    base_dir = Path(".")
    cities_df    = prep_cities_base(Path(args.cities))
    households_df = prep_istat(read_csv(Path(args.households)))
    homes_df      = prep_istat(read_csv(Path(args.homes)))
    families_df   = prep_istat(read_csv(Path(args.families)))
    commuting_df  = prep_istat(read_csv(Path(args.commuting)))

    enriched = merge_all(cities_df, households_df, homes_df, families_df, commuting_df)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(args.out, index=False, encoding="utf-8")

    # Quality report
    report = {
        "rows_base": len(cities_df),
        "rows_enriched": len(enriched),
        "matched_households": int(enriched["households"].notna().sum()) if "households" in enriched.columns else None,
        "matched_homes": int(enriched["dwellings_total"].notna().sum()) if "dwellings_total" in enriched.columns else None,
        "matched_families": int(enriched["families_total"].notna().sum()) if "families_total" in enriched.columns else None,
        "matched_commuting": int(enriched["commuting_population"].notna().sum()) if "commuting_population" in enriched.columns else None,
        "istat_codes_filled": int(enriched["istat_comune_code"].notna().sum()) if "istat_comune_code" in enriched.columns else None,
    }
    # Print a compact summary
    print("=== Merge Summary ===")
    for k, v in report.items():
        print(f"{k}: {v}")
    print(f"\nSaved to: {args.out}")

if __name__ == "__main__":
    main()
