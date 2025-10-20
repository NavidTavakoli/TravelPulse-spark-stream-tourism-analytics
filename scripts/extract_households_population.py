#!/usr/bin/env python3
import os, re, csv
import pandas as pd

DATA_DIR = "/home/tahmast/Projects/Tourism/Dataset"
FAM_POP_PATH = os.path.join(DATA_DIR, "DCSS_FAM_POP - Numero di famiglie e popolazione residente in famiglia - intero ds.csv")
POP_DEM_PATH = os.path.join(DATA_DIR, "DCSS_POP_DEMCITMIG - Caratteristiche demografiche e cittadinanza - intero ds.csv")

OUTDIR = "/home/tahmast/Projects/Tourism/data/curated"
os.makedirs(OUTDIR, exist_ok=True)

CAND_SEPARATORS = [';', '|', ',', '\t']  

def sniff_sep(path, sample_bytes=2048):
    with open(path, 'rb') as f:
        sample = f.read(sample_bytes).decode('utf-8', errors='ignore')

    counts = {sep: sample.count(sep) for sep in CAND_SEPARATORS}
    return max(counts, key=counts.get)

def read_istat_auto(path):
    sep = sniff_sep(path)
    df = pd.read_csv(
        path,
        sep=sep,
        dtype=str,
        engine='python',
        quoting=csv.QUOTE_NONE,   
        on_bad_lines='skip',
        encoding='utf-8',
        na_filter=False
    )

    df.columns = [c.strip().strip('"').strip() for c in df.columns]

    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].str.strip().str.strip('"').str.strip()
    return df

def find_col(df, candidates):

    cols_norm = {re.sub(r'\W+', '', c.lower()): c for c in df.columns}
    for cand in candidates:
        key = re.sub(r'\W+', '', cand.lower())
        if key in cols_norm:
            return cols_norm[key]

    for c in df.columns:
        if any(re.sub(r'\W+','',cand.lower()) in re.sub(r'\W+','',c.lower()) for cand in candidates):
            return c
    raise KeyError(f"Could not find any of columns: {candidates} in {list(df.columns)[:15]}...")

def coerce_num(s):
    return pd.to_numeric(
        s.str.replace('\u00A0', '', regex=False)  
         .str.replace('.', '', regex=False)       
         .str.replace(',', '.', regex=False),    
        errors='coerce'
    )

def latest_year(df, time_col):
    years = pd.to_numeric(df[time_col], errors='coerce')
    return int(years.max())

def extract_households():
    df = read_istat_auto(FAM_POP_PATH)

    col_code   = find_col(df, ['ITTER107','COD_COMUNE','Codice territorio'])
    col_name   = find_col(df, ['Territorio','Denominazione territorio','Comune'])
    col_type   = find_col(df, ['TIPO_DATO_CENS_POP','Tipo dato'])
    col_time   = find_col(df, ['TIME','Anno','Seleziona periodo'])
    col_value  = find_col(df, ['Value','Valore'])

    mask_hh = df[col_type].str.contains(r'\bNPHH_AV\b', case=False, na=False)
    df = df[mask_hh].copy()

    year = latest_year(df, col_time)
    df = df[df[col_time] == str(year)].copy()

    df['value_num'] = coerce_num(df[col_value]).fillna(0).astype('int64')


    is_macro = df[col_code].str.fullmatch(r'IT[A-Z]{0,2}\d{0,2}', na=False)
    out = df[~is_macro][[col_code, col_name, col_time, 'value_num']].rename(
        columns={col_code:'territory_code', col_name:'territory_name', col_time:'year', 'value_num':'households'}
    )
    out.to_csv(os.path.join(OUTDIR, 'istat_households.csv'), index=False, encoding='utf-8')
    print(f"✓ households: {len(out)} rows (year={year})  sep_detected={sniff_sep(FAM_POP_PATH)}")

def extract_population():
    df = read_istat_auto(POP_DEM_PATH)

    col_code   = find_col(df, ['ITTER107','COD_COMUNE','Codice territorio'])
    col_name   = find_col(df, ['Territorio','Denominazione territorio','Comune'])
    col_type   = find_col(df, ['TIPO_DATO_CENS_POP','Tipo dato'])
    col_time   = find_col(df, ['TIME','Anno','Seleziona periodo'])
    col_value  = find_col(df, ['Value','Valore'])


    mask_pop = df[col_type].str.contains(r'\bRESPOP_AV\b|\bRESPOP_MIN_AV\b', case=False, na=False)
    df = df[mask_pop].copy()

    year = latest_year(df, col_time)
    df = df[df[col_time] == str(year)].copy()

    df['value_num'] = coerce_num(df[col_value]).fillna(0).astype('int64')

    is_macro = df[col_code].str.fullmatch(r'IT[A-Z]{0,2}\d{0,2}', na=False)
    out = df[~is_macro][[col_code, col_name, col_time, 'value_num']].rename(
        columns={col_code:'territory_code', col_name:'territory_name', col_time:'year', 'value_num':'population'}
    )
    out.to_csv(os.path.join(OUTDIR, 'istat_population.csv'), index=False, encoding='utf-8')
    print(f"✓ population: {len(out)} rows (year={year})  sep_detected={sniff_sep(POP_DEM_PATH)}")

if __name__ == "__main__":
    extract_households()
    extract_population()
