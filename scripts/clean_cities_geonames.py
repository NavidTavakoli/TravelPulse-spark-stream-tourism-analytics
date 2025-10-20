#!/usr/bin/env python3
# GeoNames IT.txt -> data/curated/cities_it.csv


import os, sys, csv
import pandas as pd

IN  = sys.argv[1] if len(sys.argv) > 1 else "/home/tahmast/Projects/Tourism/Dataset/IT/IT.txt"
OUT = "/home/tahmast/Projects/Tourism/data/curated/cities_it.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)

COLS = [
    "geonameid","name","asciiname","alternatenames","latitude","longitude",
    "feature_class","feature_code","country_code","cc2",
    "admin1_code","admin2_code","admin3_code","admin4_code",
    "population","elevation","dem","timezone","modification_date"
]

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

# fallback: admin1_code (01..20) → Region
ADMIN1_TO_REGION = {
    "01":"Piemonte","02":"Valle d'Aosta/Vallée d'Aoste","03":"Lombardia","04":"Trentino-Alto Adige/Südtirol",
    "05":"Veneto","06":"Friuli-Venezia Giulia","07":"Liguria","08":"Emilia-Romagna","09":"Toscana",
    "10":"Umbria","11":"Marche","12":"Lazio","13":"Abruzzo","14":"Molise","15":"Campania","16":"Puglia",
    "17":"Basilicata","18":"Calabria","19":"Sicilia","20":"Sardegna","00":"(Unknown)"
}

CHUNK_MODE = False
CHUNK_SIZE = 200_000

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df["feature_class"] == "P") & (df["feature_code"].isin(["PPLC","PPLA","PPLA2","PPLA3","PPLA4","PPL"]))].copy()

    df["lat"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["lon"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["population"] = pd.to_numeric(df["population"], errors="coerce").fillna(0).astype("int64")
    df = df[df["lat"].between(-90, 90) & df["lon"].between(-180, 180) & (df["population"] > 0)]

    df["region"] = df["admin2_code"].map(PROV_CODE_TO_REGION)
    df["admin1_code"] = df["admin1_code"].astype(str).str.zfill(2)
    df.loc[df["region"].isna(), "region"] = df["admin1_code"].map(ADMIN1_TO_REGION)
    df["region"] = df["region"].fillna("(Unknown)")

    df = df.sort_values(["name","population"], ascending=[True, False])\
           .drop_duplicates(subset=["name","admin1_code"], keep="first")

    out = df.rename(columns={"geonameid":"city_id","name":"city_name"})[
        ["city_id","city_name","admin1_code","region","lat","lon","population"]
    ].copy()
    out["city_name"] = out["city_name"].str.strip()
    return out

def main():
    if CHUNK_MODE:
        first = True
        for chunk in pd.read_csv(IN, sep="\t", header=None, names=COLS,
                                 dtype=str, quoting=csv.QUOTE_NONE,
                                 keep_default_na=False, chunksize=CHUNK_SIZE, low_memory=False):
            out = normalize(chunk)
            out.to_csv(OUT, index=False, mode="w" if first else "a", header=first, encoding="utf-8")
            first = False
        print(f"✓ Saved (chunked): {OUT}")
    else:
        df = pd.read_csv(IN, sep="\t", header=None, names=COLS,
                         dtype=str, quoting=csv.QUOTE_NONE,
                         keep_default_na=False, low_memory=False)
        out = normalize(df).sort_values(["region","population"], ascending=[True, False]).reset_index(drop=True)
        os.makedirs(os.path.dirname(OUT), exist_ok=True)
        out.to_csv(OUT, index=False, encoding="utf-8")
        print(f"✓ Saved: {OUT}  rows={len(out)}")
        print(out.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
