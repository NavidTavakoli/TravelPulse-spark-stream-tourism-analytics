#!/usr/bin/env python3
import csv, re
import pandas as pd

CAND_SEPARATORS = [';', '|', ',', '\t']

def sniff_sep(path, sample_bytes=4096):
    with open(path, 'rb') as f:
        sample = f.read(sample_bytes).decode('utf-8', errors='ignore')
    counts = {sep: sample.count(sep) for sep in CAND_SEPARATORS}
    return max(counts, key=counts.get)

def read_istat_auto(path):
    sep = sniff_sep(path)
    df = pd.read_csv(
        path, sep=sep, dtype=str, engine='python',
        quoting=csv.QUOTE_NONE, on_bad_lines='skip',
        encoding='utf-8', na_filter=False
    )

    df.columns = [c.strip().strip('"').strip() for c in df.columns]

    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].str.strip().str.strip('"').str.strip()
    return df

def find_col(df, candidates):
    import re
    cols_norm = {re.sub(r'\W+','', c.lower()): c for c in df.columns}
    for cand in candidates:
        key = re.sub(r'\W+','', cand.lower())
        if key in cols_norm: return cols_norm[key]
    for c in df.columns:  
        if any(re.sub(r'\W+','',cand.lower()) in re.sub(r'\W+','',c.lower()) for cand in candidates):
            return c
    raise KeyError(f"Could not find any of columns {candidates} in {df.columns[:12].tolist()} ...")

def coerce_num_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.str.replace('\u00A0','', regex=False)  
         .str.replace('.', '', regex=False)      
         .str.replace(',', '.', regex=False),    
        errors='coerce'
    )

def is_macro_territory(code: pd.Series) -> pd.Series:

    return code.str.fullmatch(r'IT[A-Z]{0,2}\d{0,2}', na=False)
