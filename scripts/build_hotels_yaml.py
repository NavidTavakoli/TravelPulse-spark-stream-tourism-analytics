#!/usr/bin/env python3
# -*- coding: utf-8 -*-



import argparse
import json
import math
import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


# ----------------------------- Utils ----------------------------- #

def norm_text(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s)
                if not unicodedata.combining(c))
    s = re.sub(r"[’'`]", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()


def slug(s: str) -> str:
    t = norm_text(s)
    return t.replace(" ", "-")[:40]


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def parse_wkt_point(wkt: str):
    """Parse 'POINT (lon lat)' → (lat, lon) or (nan, nan) if not parseable."""
    if pd.isna(wkt):
        return (np.nan, np.nan)
    m = re.search(r"POINT\s*\(\s*([-\d\.eE]+)\s+([-\d\.eE]+)\s*\)", str(wkt))
    if not m:
        return (np.nan, np.nan)
    lon = float(m.group(1))
    lat = float(m.group(2))
    return (lat, lon)


def first_present(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def any_column_like(df: pd.DataFrame, patterns):
    cols = list(df.columns)
    for p in patterns:
        rx = re.compile(p, flags=re.IGNORECASE)
        for c in cols:
            if rx.search(c):
                return c
    return None


def to_float_or_nan(x):
    try:
        return float(x)
    except Exception:
        return np.nan


def to_stars(v):
    """Normalize stars '★★★', '3', '3.5', 'hotel 4*' → float or NaN."""
    if pd.isna(v):
        return np.nan
    s = str(v).strip().lower().replace("★", "").replace("☆", "")
    m = re.search(r"([0-5](?:\.\d)?)", s)
    return float(m.group(1)) if m else np.nan


def ensure_float_series(x):
    return pd.to_numeric(x, errors="coerce").astype(float)


# ----------------------------- Loaders ----------------------------- #

def load_hotels_from_csv(csv_path: Path) -> pd.DataFrame:
    """Load hotels from CSV, resolving columns and extracting coordinates (lon/lat | X/Y | WKT)."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path, dtype=str)

    # Try various lon/lat combinations (lon,lat) or (X,Y) or (POINT_X,POINT_Y)
    lon_col = first_present(df, ["lon", "Lon", "LON", "X", "x", "POINT_X", "point_x"])
    lat_col = first_present(df, ["lat", "Lat", "LAT", "Y", "y", "POINT_Y", "point_y"])

    # If none, try WKT-like columns
    if lon_col is None or lat_col is None:
        wkt_col = first_present(df, ["WKT", "wkt", "geom", "geometry", "OGRGeometry", "GEOMETRY"])
        if wkt_col is not None:
            coords = df[wkt_col].map(parse_wkt_point)
            df["lat"] = coords.map(lambda t: t[0])
            df["lon"] = coords.map(lambda t: t[1])
            lat_col, lon_col = "lat", "lon"

    if lon_col is None or lat_col is None:
        # No geometry in CSV → let caller fallback to GeoJSON
        raise RuntimeError("CSV has no recognizable lon/lat or WKT geometry")

    # Semantic columns (robust)
    name_col = first_present(df, ["tags.name", "name", "Name"])
    kind_col = first_present(df, ["tags.tourism", "tourism", "kind", "Kind"])
    stars_col = first_present(df, ["tags.stars", "stars", "Stars"])
    addr_city_col = first_present(df, ["tags.addr:city", "addr:city", "addr_city", "city", "City"])
    addr_street_col = first_present(df, ["tags.addr:street", "addr:street", "addr_street", "street", "Street"])
    phone_col = any_column_like(df, [r"^tags\.phone$", r"(^|:)phone", r"contact.*phone"])
    website_col = any_column_like(df, [r"website", r"url"])
    osm_type_col = first_present(df, ["type", "Type"])
    osm_id_col = first_present(df, ["id", "osmid", "OSMID", "@id"])

    for must in [name_col, kind_col]:
        if must is None:
            raise RuntimeError("Missing essential columns (need at least name, kind).")

    out = pd.DataFrame({
        "name": df[name_col].fillna("").astype(str).str.strip(),
        "kind": df[kind_col].astype(str).str.strip(),
        "lat": ensure_float_series(df[lat_col]),
        "lon": ensure_float_series(df[lon_col]),
        "stars": (df[stars_col] if stars_col else np.nan),
        "addr_city": (df[addr_city_col] if addr_city_col else ""),
        "addr_street": (df[addr_street_col] if addr_street_col else ""),
        "phone": (df[phone_col] if phone_col else ""),
        "website": (df[website_col] if website_col else ""),
        "osm_type": (df[osm_type_col] if osm_type_col else ""),
        "osm_id": (df[osm_id_col] if osm_id_col else ""),
    })

    out = out.dropna(subset=["lat", "lon"])
    out["name"] = out["name"].fillna("").str.strip()
    out = out[out["name"] != ""]
    out["stars_num"] = out["stars"].map(to_stars)
    out["addr_city_norm"] = out["addr_city"].map(norm_text)
    return out


def load_hotels_from_geojsonl(geojsonl_path: Path) -> pd.DataFrame:
    """Parse line-delimited GeoJSON (GeoJSONSeq) and extract properties + point geometry."""
    if not geojsonl_path.exists():
        raise FileNotFoundError(f"GeoJSONSeq not found: {geojsonl_path}")

    rows = []
    with geojsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                feat = json.loads(line)
            except json.JSONDecodeError:
                continue

            props = feat.get("properties", {}) or {}
            geom = feat.get("geometry", {}) or {}

            # geometry → lon,lat
            lon = lat = np.nan
            if geom and geom.get("type") == "Point":
                coords = geom.get("coordinates", [])
                if len(coords) >= 2:
                    lon, lat = coords[0], coords[1]

            # Map common fields (props may have 'tags.name' or 'name')
            name = props.get("tags.name") or props.get("name") or ""
            kind = props.get("tags.tourism") or props.get("tourism") or props.get("kind") or ""
            stars = props.get("tags.stars") or props.get("stars")
            addr_city = props.get("tags.addr:city") or props.get("addr:city") or ""
            addr_street = props.get("tags.addr:street") or props.get("addr:street") or ""
            phone = props.get("tags.phone") or props.get("phone") or props.get("contact:phone") or ""
            website = props.get("tags.website") or props.get("website") or props.get("url") or ""
            osm_type = props.get("type") or ""
            osm_id = props.get("id") or props.get("@id") or ""

            rows.append({
                "name": str(name).strip(),
                "kind": str(kind).strip(),
                "lat": to_float_or_nan(lat),
                "lon": to_float_or_nan(lon),
                "stars": stars,
                "addr_city": addr_city,
                "addr_street": addr_street,
                "phone": phone,
                "website": website,
                "osm_type": osm_type,
                "osm_id": osm_id,
            })

    df = pd.DataFrame(rows)
    df = df.dropna(subset=["lat", "lon"])
    df["name"] = df["name"].fillna("").str.strip()
    df = df[df["name"] != ""]
    df["stars_num"] = df["stars"].map(to_stars)
    df["addr_city_norm"] = df["addr_city"].map(norm_text)
    return df


def load_cities(cities_csv: Path) -> pd.DataFrame:
    c = pd.read_csv(cities_csv, dtype=str)
    for col in ["lat", "lon", "population"]:
        if col in c.columns:
            c[col] = pd.to_numeric(c[col], errors="coerce")
    c["city_name_norm"] = c["city_name"].map(norm_text)
    return c[["city_id", "city_name", "city_name_norm", "region", "lat", "lon"]]


# ----------------------------- Processing ----------------------------- #

def match_cities(hotels_df: pd.DataFrame, cities_df: pd.DataFrame) -> pd.DataFrame:
    merged = hotels_df.merge(
        cities_df,
        left_on="addr_city_norm",
        right_on="city_name_norm",
        how="left",
        suffixes=("", "_city"),
    )

    mask_un = merged["city_id"].isna()
    if mask_un.any():
        city_points = cities_df.dropna(subset=["lat", "lon"]).reset_index(drop=True)
        c_lat = city_points["lat"].to_numpy()
        c_lon = city_points["lon"].to_numpy()
        cid = city_points["city_id"].to_numpy()
        cname = city_points["city_name"].to_numpy()
        creg = city_points["region"].to_numpy()

        matched_id, matched_name, matched_reg = [], [], []
        for lat, lon in merged.loc[mask_un, ["lat", "lon"]].to_numpy():
            d = (c_lat - lat) ** 2 + (c_lon - lon) ** 2
            j = int(np.argmin(d))
            matched_id.append(cid[j])
            matched_name.append(cname[j])
            matched_reg.append(creg[j])

        merged.loc[mask_un, "city_id"] = matched_id
        merged.loc[mask_un, "city_name"] = matched_name
        merged.loc[mask_un, "region"] = matched_reg

    return merged


def dedup_hotels(df: pd.DataFrame, radius_km: float = 0.2) -> pd.DataFrame:
    df = df.copy()
    df["name_norm"] = df["name"].map(norm_text)
    df["group_key"] = df["city_id"].astype(str) + "|" + df["name_norm"]

    def dedup_group(g: pd.DataFrame) -> pd.DataFrame:
        g = g.reset_index(drop=True)
        keep_idx = []
        used = np.zeros(len(g), dtype=bool)
        coords = g[["lat", "lon"]].to_numpy()
        for i in range(len(g)):
            if used[i]:
                continue
            used[i] = True
            keep_idx.append(i)
            for j in range(i + 1, len(g)):
                if used[j]:
                    continue
                if haversine_km(coords[i][0], coords[i][1], coords[j][0], coords[j][1]) < radius_km:
                    used[j] = True
        return g.iloc[keep_idx]

    return df.groupby("group_key", group_keys=False).apply(dedup_group).reset_index(drop=True)


def build_ids(df: pd.DataFrame) -> pd.DataFrame:
    suffix = df.get("osm_id", pd.Series([""] * len(df))).astype(str).str[-6:].replace("nan", "")
    fallback = df.index.astype(str).str.zfill(6)
    tail = np.where(suffix.str.len() > 0, suffix, fallback)
    df["hotel_id"] = df.apply(lambda r: f"{r['city_id']}-{slug(r['name'])}-{tail[r.name]}", axis=1)
    return df


def write_outputs(df: pd.DataFrame, out_csv: Path, out_yaml: Path):
    cols = [
        "hotel_id", "name", "kind", "stars_num",
        "city_id", "city_name", "region", "lat", "lon",
        "addr_street", "phone", "website"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    df[cols].to_csv(out_csv, index=False)

    payload = {
        "schema_version": "v1",
        "source": "OSM_italy_251017",
        "generated_by": "build_hotels_yaml.py",
        "hotels": []
    }
    for _, r in df.iterrows():
        payload["hotels"].append({
            "hotel_id": r["hotel_id"],
            "name": r["name"],
            "type": r["kind"],
            "stars": (None if pd.isna(r["stars_num"]) else float(r["stars_num"])),
            "city_id": str(r["city_id"]),
            "city_name": r["city_name"],
            "region": r["region"],
            "lat": float(r["lat"]),
            "lon": float(r["lon"]),
            "addr_street": (None if pd.isna(r["addr_street"]) else str(r["addr_street"])),
            "phone": (None if pd.isna(r["phone"]) else str(r["phone"])),
            "website": (None if pd.isna(r["website"]) else str(r["website"]))
        })

    out_yaml.parent.mkdir(parents=True, exist_ok=True)
    with open(out_yaml, "w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, sort_keys=False, allow_unicode=True)


# ----------------------------- CLI ----------------------------- #

def main():
    ap = argparse.ArgumentParser(description="Build hotels.yaml and hotels_clean.csv from OSM-exported data (CSV/GeoJSONSeq).")
    ap.add_argument("--base", default="/home/tahmast/Projects/Tourism",
                    help="Project base path (default: /home/tahmast/Projects/Tourism)")
    ap.add_argument("--hotels-csv", default=None,
                    help="Path to hotels_raw.csv (default: {BASE}/data/curated/hotels_raw.csv)")
    ap.add_argument("--hotels-geojsonl", default=None,
                    help="Path to hotels_raw.geojsonl fallback (default: {BASE}/data/curated/hotels_raw.geojsonl)")
    ap.add_argument("--cities-csv", default="/home/tahmast/Projects/Tourism/data/curated/cities_it_enriched_cleaned.csv",
                    help="Path to cities_it_enriched_cleaned.csv")
    ap.add_argument("--out-csv", default=None,
                    help="Output CSV (default: {BASE}/data/curated/hotels_clean.csv)")
    ap.add_argument("--out-yaml", default=None,
                    help="Output YAML (default: {BASE}/configs/hotels.yaml)")
    ap.add_argument("--dedup-radius-km", type=float, default=0.2,
                    help="Dedup radius (km) for same-name hotels in a city (default: 0.2)")
    args = ap.parse_args()

    base = Path(args.base)
    hotels_csv = Path(args.hotels_csv) if args.hotels_csv else base / "data/curated/hotels_raw.csv"
    hotels_geojsonl = Path(args.hotels_geojsonl) if args.hotels_geojsonl else base / "data/curated/hotels_raw.geojsonl"
    cities_csv = Path(args.cities_csv)
    out_csv = Path(args.out_csv) if args.out_csv else base / "data/curated/hotels_clean.csv"
    out_yaml = Path(args.out_yaml) if args.out_yaml else base / "configs/hotels.yaml"

    # Load hotels (CSV → fallback GeoJSONSeq)
    try:
        print(f"→ Loading hotels from CSV: {hotels_csv}")
        hotels = load_hotels_from_csv(hotels_csv)
    except Exception as e_csv:
        print(f"CSV load failed: {e_csv}\n→ Falling back to GeoJSONSeq: {hotels_geojsonl}")
        hotels = load_hotels_from_geojsonl(hotels_geojsonl)

    # Load cities
    print(f"→ Loading cities: {cities_csv}")
    cities = load_cities(cities_csv)

    # Match cities
    print("→ Matching hotels to cities (name-based, then nearest fallback)…")
    merged = match_cities(hotels, cities)

    # Dedup & IDs
    print(f"→ Deduplicating hotels within {args.dedup_radius_km} km …")
    deduped = dedup_hotels(merged, radius_km=args.dedup_radius_km)
    with_ids = build_ids(deduped)

    # Outputs
    print(f"→ Writing outputs:\n   CSV:  {out_csv}\n   YAML: {out_yaml}")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    write_outputs(with_ids, out_csv, out_yaml)

    print(f"✓ Done. Hotels written: {len(with_ids)}")


if __name__ == "__main__":
    main()
