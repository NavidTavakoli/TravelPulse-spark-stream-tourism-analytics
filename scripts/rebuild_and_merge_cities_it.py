#!/usr/bin/env python3



import os
import re
import csv
import argparse
import unicodedata
import numpy as np
import pandas as pd


DEF_IT = "/home/tahmast/Projects/Tourism/Dataset/IT/IT.txt"
DEF_CURATED = "/home/tahmast/Projects/Tourism/data/curated"
DEF_MANUAL = None  # "/home/tahmast/Projects/Tourism/data/curated/manual_city_mapping.csv"

PROV_CODE_TO_REGION = {
    # Piemonte
    "AL":"Piemonte","AT":"Piemonte","BI":"Piemonte","CN":"Piemonte","NO":"Piemonte","TO":"Piemonte","VB":"Piemonte","VC":"Piemonte",
    # Valle d'Aosta
    "AO":"Valle d'Aosta/Vallée d'Aoste",
    # Lombardia
    "BG":"Lombardia","BS":"Lombardia","CO":"Lombardia","CR":"Lombardia","LC":"Lombardia","LO":"Lombardia","MB":"Lombardia",
    "MI":"Lombardia","MN":"Lombardia","PV":"Lombardia","SO":"Lombardia","VA":"Lombardia",
    # Trentino-Alto Adige/Südtirol
    "BZ":"Trentino-Alto Adige/Südtirol","TN":"Trentino-Alto Adige/Südtirol",
    # Veneto
    "BL":"Veneto","PD":"Veneto","RO":"Veneto","TV":"Veneto","VE":"Veneto","VR":"Veneto","VI":"Veneto",
    # Friuli-Venezia Giulia
    "GO":"Friuli-Venezia Giulia","PN":"Friuli-Venezia Giulia","TS":"Friuli-Venezia Giulia","UD":"Friuli-Venezia Giulia",
    # Liguria
    "GE":"Liguria","IM":"Liguria","SP":"Liguria","SV":"Liguria",
    # Emilia-Romagna
    "BO":"Emilia-Romagna","FC":"Emilia-Romagna","FE":"Emilia-Romagna","MO":"Emilia-Romagna","PR":"Emilia-Romagna",
    "PC":"Emilia-Romagna","RA":"Emilia-Romagna","RE":"Emilia-Romagna","RN":"Emilia-Romagna",
    # Toscana
    "AR":"Toscana","FI":"Toscana","GR":"Toscana","LI":"Toscana","LU":"Toscana","MS":"Toscana","PI":"Toscana",
    "PT":"Toscana","PO":"Toscana","SI":"Toscana",
    # Umbria
    "PG":"Umbria","TR":"Umbria",
    # Marche
    "AN":"Marche","AP":"Marche","FM":"Marche","MC":"Marche","PU":"Marche",
    # Lazio
    "FR":"Lazio","LT":"Lazio","RI":"Lazio","RM":"Lazio","VT":"Lazio",
    # Abruzzo
    "AQ":"Abruzzo","CH":"Abruzzo","PE":"Abruzzo","TE":"Abruzzo",
    # Molise
    "CB":"Molise","IS":"Molise",
    # Campania
    "AV":"Campania","BN":"Campania","CE":"Campania","NA":"Campania","SA":"Campania",
    # Puglia
    "BA":"Puglia","BT":"Puglia","BR":"Puglia","FG":"Puglia","LE":"Puglia","TA":"Puglia",
    # Basilicata
    "MT":"Basilicata","PZ":"Basilicata",
    # Calabria
    "CS":"Calabria","CZ":"Calabria","KR":"Calabria","RC":"Calabria","VV":"Calabria",
    # Sicilia
    "AG":"Sicilia","CL":"Sicilia","CT":"Sicilia","EN":"Sicilia","ME":"Sicilia","PA":"Sicilia","RG":"Sicilia","SR":"Sicilia","TP":"Sicilia",
    # Sardegna 
    "CA":"Sardegna","NU":"Sardegna","OR":"Sardegna","OT":"Sardegna","OG":"Sardegna","VS":"Sardegna","SS":"Sardegna","SU":"Sardegna",
}

# Fallback: admin1_code (01..20) → Region
ADMIN1_TO_REGION = {
    "01":"Piemonte","02":"Valle d'Aosta/Vallée d'Aoste","03":"Lombardia","04":"Trentino-Alto Adige/Südtirol",
    "05":"Veneto","06":"Friuli-Venezia Giulia","07":"Liguria","08":"Emilia-Romagna","09":"Toscana",
    "10":"Umbria","11":"Marche","12":"Lazio","13":"Abruzzo","14":"Molise","15":"Campania","16":"Puglia",
    "17":"Basilicata","18":"Calabria","19":"Sicilia","20":"Sardegna","00":"(Unknown)"
}
REGION_WHITELIST = set(ADMIN1_TO_REGION.values()) | {"(Unknown)"}

# EN↔IT aliases for major city names (normalized, lowercase)
EN_IT_ALIAS = {
    "rome": "roma", "milan": "milano", "naples": "napoli", "turin": "torino",
    "florence": "firenze", "venice": "venezia", "genoa": "genova",
    "padua": "padova", "leghorn": "livorno", "syracuse": "siracusa",
}

# ------------ Helpers ------------
def norm_text(s: str) -> str:
    if s is None or (isinstance(s, float) and np.isnan(s)): s = ""
    s = str(s)
    x = unicodedata.normalize("NFKD", s)
    x = "".join(ch for ch in x if not unicodedata.combining(ch))
    x = x.lower().strip()
    x = x.replace("’","'")
    x = re.sub(r"^(comune di|citta di|citt[aà] di|city of)\s+", "", x)
    for ch in [",",";","'","\"", "(",")",".","-","_","/","\\"]:
        x = x.replace(ch, " ")
    x = re.sub(r"\s+", " ", x)
    return x

def alias_it(s_norm: str) -> str:
    return EN_IT_ALIAS.get(s_norm, s_norm)

def load_it_txt(path: str) -> pd.DataFrame:
    COLS = [
        "geonameid","name","asciiname","alternatenames","latitude","longitude",
        "feature_class","feature_code","country_code","cc2",
        "admin1_code","admin2_code","admin3_code","admin4_code",
        "population","elevation","dem","timezone","modification_date"
    ]
    return pd.read_csv(path, sep="\t", header=None, names=COLS, dtype=str,
                       quoting=csv.QUOTE_NONE, keep_default_na=False, low_memory=False)

# ------------ Build clean cities from IT.txt ------------
def build_cities(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only real populated places
    df = df[(df["feature_class"] == "P") & (df["feature_code"].isin(["PPLC","PPLA","PPLA2","PPLA3","PPLA4","PPL"]))].copy()

    # Types + quality filters
    df["lat"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["lon"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["population"] = pd.to_numeric(df["population"], errors="coerce").fillna(0).astype("int64")
    df = df[df["lat"].between(-90, 90) & df["lon"].between(-180, 180) & (df["population"] > 0)]

    # Region: prefer admin2_code → Region; fallback admin1_code
    df["admin1_code"] = df["admin1_code"].astype(str).str.zfill(2)
    df["region"] = df["admin2_code"].map(PROV_CODE_TO_REGION)
    df.loc[df["region"].isna(), "region"] = df["admin1_code"].map(ADMIN1_TO_REGION)
    df["region"] = df["region"].fillna("(Unknown)")

    # Name normalization for dedup
    df["__name_norm"] = df["name"].map(norm_text)
    df = df.sort_values(["__name_norm","admin1_code","population"], ascending=[True, True, False]) \
           .drop_duplicates(subset=["__name_norm","admin1_code"], keep="first")

    out = df.rename(columns={"geonameid":"city_id","name":"city_name"})[
        ["city_id","city_name","admin1_code","region","lat","lon","population","asciiname","__name_norm","admin2_code"]
    ].copy()
    out["city_name"] = out["city_name"].str.strip()

    # multi-keys for staged merge
    out["__key_city"]  = out["city_name"].map(norm_text)
    out["__key_asci"]  = out["asciiname"].map(norm_text)
    out["__key_cityA"] = out["__key_city"].map(alias_it)
    out["__key_asciA"] = out["__key_asci"].map(alias_it)

    # sanity
    assert set(out["region"].unique()).issubset(REGION_WHITELIST), "Unexpected region names in cities_clean"
    return out

# ------------ ISTAT prep ------------
def prep_istat(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    # detect territory-name column
    name_col = None
    for c in d.columns:
        if any(tok in c.lower() for tok in ["territ", "comune", "name", "localita", "luogo"]):
            name_col = c; break
    if name_col is None: name_col = d.columns[0]
    d["__territory_name"] = d[name_col].astype(str)
    d["__key_norm"] = d["__territory_name"].map(norm_text)
    d["__key_alias"] = d["__key_norm"].map(alias_it)

    # keep latest year if time column exists
    ycol = next((c for c in ["year","time","TIME"] if c in d.columns), None)
    if ycol:
        def ex_year(x):
            m = re.search(r"(20\d{2}|19\d{2})", str(x))
            return int(m.group(1)) if m else np.nan
        d["_year_"] = d[ycol].apply(ex_year)
        d = d.sort_values("_year_").groupby("__key_norm", as_index=False).tail(1)
        d = d.drop(columns=["_year_"], errors="ignore")
    return d

def select_cols(df: pd.DataFrame, wanted: list) -> pd.DataFrame:
    lmap = {c.lower(): c for c in df.columns}
    cols = [lmap[w] for w in wanted if w in lmap]
    cols = list(dict.fromkeys(cols + ["__key_norm","__key_alias"]))
    return df[cols].copy()

# ------------ Staged merge (4 keys vs 2 keys) ------------
def staged_merge(left: pd.DataFrame, right: pd.DataFrame, payload_cols: list) -> pd.DataFrame:

    L = left.copy()

    if right is None or right.empty:
        return L

    payload_in_right = [c for c in payload_cols if c in right.columns]
    if not payload_in_right:
        return L

    for c in payload_in_right:
        if c not in L.columns:
            L[c] = np.nan

    cols_right = ["__key_norm", "__key_alias"] + payload_in_right
    cols_right = [c for c in cols_right if c in right.columns]
    base_right = right[cols_right].copy()

    def merge_once(L_in: pd.DataFrame, lk: str, rk: str) -> pd.DataFrame:
        """A merge using specified keys, filling missing values from temporary columns."""
        if rk not in base_right.columns:
            return L_in

        R = base_right.rename(columns={rk: "__rkey"}).copy()

        # rename payloads to temp to avoid overlap
        tmp_names = {c: f"__tmp_{c}" for c in payload_in_right}
        R = R[["__rkey"] + payload_in_right].rename(columns=tmp_names)

        M = L_in.merge(R, left_on=lk, right_on="__rkey", how="left")
        for c in payload_in_right:
            tmpc = f"__tmp_{c}"
            if tmpc in M.columns:
                M[c] = M[c].combine_first(M[tmpc])
                M.drop(columns=[tmpc], inplace=True)
        if "__rkey" in M.columns:
            M.drop(columns="__rkey", inplace=True)
        return M

    for lk in ["__key_city", "__key_cityA", "__key_asci", "__key_asciA"]:
        L = merge_once(L, lk, "__key_norm")
        need = L[payload_in_right].isna().all(axis=1)
        if need.any():
            sub = L.loc[need].copy()
            sub = merge_once(sub, lk, "__key_alias")
            for c in payload_in_right:
                L.loc[need, c] = L.loc[need, c].combine_first(sub[c])

    return L



# ------------ Robust merge all ISTATs ------------
def robust_merge(cities: pd.DataFrame, curated_dir: str, manual_map_path: str|None) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Load ISTAT
    hh  = pd.read_csv(os.path.join(curated_dir, "istat_households.csv"))
    hom = pd.read_csv(os.path.join(curated_dir, "istat_homes.csv"))
    fam = pd.read_csv(os.path.join(curated_dir, "istat_families.csv"))
    com = pd.read_csv(os.path.join(curated_dir, "istat_commuting.csv"))

    hh, hom, fam, com = prep_istat(hh), prep_istat(hom), prep_istat(fam), prep_istat(com)

    # Optional manual mapping (source_name,target_name)
    if manual_map_path and os.path.exists(manual_map_path):
        mp = pd.read_csv(manual_map_path)
        mp["source_key"] = mp["source_name"].map(norm_text)
        mp["target_key"] = mp["target_name"].map(norm_text)
        mpd = dict(zip(mp["source_key"], mp["target_key"]))
        for d in (hh, hom, fam, com):
            d["__key_norm"] = d["__key_norm"].map(lambda k: mpd.get(k, k))
            d["__key_alias"] = d["__key_alias"].map(lambda k: mpd.get(k, k))

    # select payloads
    hh_sel   = select_cols(hh,  ["territory_name","households","year","territory_code"])
    hom_sel  = select_cols(hom, ["territory_name","dwellings_total","dwellings_occupied","occupied_share","year"])
    fam_sel  = select_cols(fam, ["territory_name","families_total","families_3plus_share","avg_family_size_from_istat","year"])
    com_sel  = select_cols(com, ["territory_name","resident_population","commuting_population","commuting_ratio","year"])

    out = cities.copy()
    out = staged_merge(out, hh_sel,  ["households", "territory_name", "year", "territory_code"])
    out = staged_merge(out, hom_sel, ["dwellings_total","dwellings_occupied","occupied_share","territory_name","year"])
    out = staged_merge(out, fam_sel, ["families_total","families_3plus_share","avg_family_size_from_istat","territory_name","year"])
    out = staged_merge(out, com_sel, ["resident_population","commuting_population","commuting_ratio","territory_name","year"])

    # avg_family_size (fallback)
    if "avg_family_size_from_istat" in out.columns:
        out["avg_family_size"] = out["avg_family_size_from_istat"]
    pop = pd.to_numeric(out["population"], errors="coerce")
    if "households" in out.columns:
        hhv = pd.to_numeric(out["households"], errors="coerce").replace({0: np.nan})
        out["avg_family_size"] = out.get("avg_family_size", np.nan)
        out["avg_family_size"] = out["avg_family_size"].fillna(pop / hhv)

    # Fill empty city_name from asciiname
    out["city_name"] = out["city_name"].fillna(out.get("asciiname")).fillna("UNKNOWN")

    # enforce dtypes
    for c in ["lat","lon"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    for c in ["population","households","dwellings_total","dwellings_occupied","families_total",
              "resident_population","commuting_population"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    if "city_id" in out.columns:
        out["city_id"] = pd.to_numeric(out["city_id"], errors="coerce").astype("Int64")

    # unique city_id (keep richer rows → higher population)
    if "city_id" in out.columns:
        out = out.sort_values(["population"], ascending=[False]).drop_duplicates(subset=["city_id"], keep="first")

    # reorder columns
    preferred = [
        "city_id","city_name","region","lat","lon","population",
        "households","avg_family_size",
        "dwellings_total","dwellings_occupied","occupied_share",
        "families_total","families_3plus_share",
        "resident_population","commuting_population","commuting_ratio",
        "admin1_code","asciiname",
        "territory_name","year","territory_code"
    ]
    keep = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred and not c.startswith("__")]
    out_final = out[keep].copy()

    # Flags per source (which sources missing)
    has_households = out_final["households"].notna() if "households" in out_final.columns else pd.Series(False, index=out_final.index)
    has_homes = out_final[["dwellings_total","dwellings_occupied","occupied_share"]].notna().any(axis=1) if set(["dwellings_total","dwellings_occupied","occupied_share"]).issubset(out_final.columns) else pd.Series(False, index=out_final.index)
    has_families = out_final[["families_total","families_3plus_share","avg_family_size_from_istat"]].notna().any(axis=1) if set(["families_total","families_3plus_share","avg_family_size_from_istat"]).issubset(out_final.columns) else pd.Series(False, index=out_final.index)
    has_commuting = out_final[["resident_population","commuting_population","commuting_ratio"]].notna().any(axis=1) if set(["resident_population","commuting_population","commuting_ratio"]).issubset(out_final.columns) else pd.Series(False, index=out_final.index)

    miss_cols = [c for c in ["households","dwellings_total","families_total","resident_population","commuting_ratio"] if c in out_final.columns]
    miss_score = out_final[miss_cols].isna().sum(axis=1) if miss_cols else out_final.isna().sum(axis=1)

    report = pd.DataFrame({
        "city_id": out_final.get("city_id"),
        "city_name": out_final.get("city_name"),
        "region": out_final.get("region"),
        "population": out_final.get("population"),
        "has_households": has_households.astype(int),
        "has_homes": has_homes.astype(int),
        "has_families": has_families.astype(int),
        "has_commuting": has_commuting.astype(int),
        "__missing_count": miss_score
    }).sort_values(["__missing_count","population"], ascending=[False, False]).head(500)

    assert set(out_final["region"].unique()).issubset(REGION_WHITELIST), "Unexpected region names in enriched"
    return out_final, report

# ------------ CLI & main ------------
def parse_args():
    p = argparse.ArgumentParser(description="Rebuild clean cities and merge ISTAT references (robust).")
    p.add_argument("--it", default=DEF_IT, help="Path to GeoNames IT.txt")
    p.add_argument("--curated", default=DEF_CURATED, help="Curated dir (ISTAT CSVs + outputs)")
    p.add_argument("--manual", default=DEF_MANUAL, help="Optional manual mapping CSV (source_name,target_name)")
    return p.parse_args()

def main():
    args = parse_args()
    it_path, curated, manual = args.it, args.curated, args.manual

    os.makedirs(curated, exist_ok=True)
    reports_dir = os.path.join(curated, "reports")
    os.makedirs(reports_dir, exist_ok=True)

    # 1) Build clean cities from IT.txt
    it_raw = load_it_txt(it_path)
    cities_clean = build_cities(it_raw)
    cities_out = os.path.join(curated, "cities_it.csv")
    cities_clean.to_csv(cities_out, index=False, encoding="utf-8")
    print(f"[OK] cities written: {cities_out} | rows={len(cities_clean):,}")

    # 2) Robust merge with ISTAT CSVs
    enriched, unmatched = robust_merge(cities_clean, curated, manual)
    enriched_out = os.path.join(curated, "cities_it_enriched_full.csv")
    unmatched_out = os.path.join(reports_dir, "unmatched_top500.csv")
    enriched.to_csv(enriched_out, index=False, encoding="utf-8")
    unmatched.to_csv(unmatched_out, index=False, encoding="utf-8")

    # quick stats
    bad_region_rows = (~enriched["region"].isin(REGION_WHITELIST)).sum() if "region" in enriched.columns else -1
    dupe_city_ids = enriched["city_id"].duplicated().sum() if "city_id" in enriched.columns else -1
    zero_pop = (pd.to_numeric(enriched["population"], errors="coerce")==0).sum() if "population" in enriched.columns else -1
    print(f"[OK] enriched: {enriched_out} | rows={len(enriched):,} | cols={len(enriched.columns):,}")
    print(f"[OK] unmatched: {unmatched_out} | rows={len(unmatched):,}")
    print(f"[CHECK] bad_region_rows={bad_region_rows}  dupe_city_ids={dupe_city_ids}  zero_population_rows={zero_pop}")

if __name__ == "__main__":
    main()
