#!/usr/bin/env python3


import os, csv, sys
from collections import defaultdict
import pandas as pd


IN  = "/home/tahmast/Projects/Tourism/Dataset/DCSS_ISTR_LAV_PEN_2 - Istruzione, lavoro e spostamenti per studio o lavoro - intero ds.csv"
OUT = "/home/tahmast/Projects/Tourism/data/curated/istat_commuting.csv"
CHUNK = 150_000  
TARGET = {
    "RESPOP_AV":  "resident_population",
    "RP_COM_DAY": "commuting_population",
}
FALLBACK_POLICY = "latest"  


def sniff_sep(path: str, sample_bytes: int = 1024 * 50) -> str:
    cand = ['|', ',', ';', '\t']
    with open(path, 'rb') as f:
        buf = f.read(sample_bytes).decode('utf-8', errors='ignore')
    scores = {s: buf.count(s) for s in cand}
    return max(scores, key=scores.get)

def norm_header(s: str) -> str:
    return str(s).strip().strip('"').strip()

def extract_years(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.replace('"','', regex=False)
    y = s.str.extract(r'(?<!\d)(20\d{2})(?!\d)')[0]
    return pd.to_numeric(y, errors='coerce')

def safe_to_numeric(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    s = s.str.replace('"', '', regex=False).str.replace('\u00A0', ' ', regex=False).str.strip()
    has_comma = s.str.contains(',', na=False)
    has_dot   = s.str.contains(r'\.', na=False)
    s = s.mask(has_comma & ~has_dot, s.str.replace(',', '.', regex=False))
    s = s.str.replace(' ', '', regex=False)
    return pd.to_numeric(s, errors='coerce')

def preview_file(path: str, sep: str, n: int = 3):
    df = pd.read_csv(
        path, sep=sep, dtype=str, engine="c",
        quoting=csv.QUOTE_NONE, on_bad_lines="skip",
        nrows=n, encoding="utf-8", header=0, low_memory=False
    )
    df.columns = [norm_header(c) for c in df.columns]
    print("\n— Preview (first", n, "rows) —")
    print(df.head(n).to_string(index=False))
    print("\n— Columns —")
    print(list(df.columns))

def find_col(df_head: pd.DataFrame, candidates: list[str]) -> str:
    cols_norm = {norm_header(c): c for c in df_head.columns}
    for cand in candidates:
        key = norm_header(cand)
        if key in cols_norm:
            return cols_norm[key]
    lower_map = {norm_header(c).lower(): c for c in df_head.columns}
    for cand in candidates:
        k = norm_header(cand).lower()
        if k in lower_map:
            return lower_map[k]
    raise KeyError(f"None of the names in {candidates} were found in the header.")


def main():
    if not os.path.exists(IN):
        print(f"No input found: {IN}")
        sys.exit(2)

    os.makedirs(os.path.dirname(OUT), exist_ok=True)

    sep = sniff_sep(IN)
    try:
        preview_file(IN, sep, n=3)
    except Exception as e:
        print("⚠️  Preview failed:", e)


    head = pd.read_csv(
        IN, sep=sep, dtype=str, engine="c",
        quoting=csv.QUOTE_NONE, on_bad_lines="skip",
        nrows=2000, encoding="utf-8", header=0, low_memory=False
    )
    head.columns = [norm_header(c) for c in head.columns]

    col_code = norm_header(find_col(head, ['ITTER107','Codice territorio']))
    col_name = norm_header(find_col(head, ['Territorio','Denominazione territorio','Comune']))
    col_type = norm_header(find_col(head, ['TIPO_DATO_CENS_POP','Tipo dato']))
    col_time = norm_header(find_col(head, ['TIME','Anno','Seleziona periodo']))
    col_val  = norm_header(find_col(head, ['Value','Valore']))

    print("→ resolved columns:", [col_code, col_name, col_type, col_time, col_val], "| sep:", repr(sep))


    def usecols_first_pass_callable(x: str) -> bool:
        xn = norm_header(x)
        return xn in {col_type, col_time}

    def usecols_second_pass_callable(x: str) -> bool:
        xn = norm_header(x)
        return xn in {col_code, col_name, col_type, col_time, col_val}


    desired_metrics = list(TARGET.keys())
    years_by_metric: dict[str, set[int]] = defaultdict(set)
    all_years_seen = set()

    for c in pd.read_csv(
        IN, sep=sep, dtype=str, engine="c",
        quoting=csv.QUOTE_NONE, on_bad_lines="skip",
        chunksize=CHUNK, encoding="utf-8", header=0, usecols=usecols_first_pass_callable,
        low_memory=False
    ):

        c.columns = [norm_header(x) for x in c.columns]
        if col_time not in c.columns or col_type not in c.columns:
            continue

        t = c[col_type].astype(str).str.strip().str.strip('"').str.strip()
        y = extract_years(c[col_time])

        all_years_seen.update(y.dropna().astype(int).unique().tolist())

        for m in desired_metrics:
            mask = (t == m) & y.notna()
            if mask.any():
                years_by_metric[m].update(y[mask].astype(int).unique())

    if not all_years_seen:
        sys.exit("No valid year was found in the dataset (the time column does not contain any year between 2000 and 2099).")


    sets_available = [years_by_metric[m] for m in desired_metrics if years_by_metric[m]]
    common_years = set.intersection(*sets_available) if sets_available else set()
    if common_years:
        target_year = max(common_years)
        print(f"→ chose target year (intersection): {target_year}")
    else:
        if FALLBACK_POLICY == "strict":
            print("❌ No common year was found among the required indicators.")
            print("   Available years per metric:", {m: sorted(list(v)) for m, v in years_by_metric.items()})
            sys.exit(1)
        target_year = max(all_years_seen)
        print("⚠️  No common year across metrics.",
              "Available:", {m: sorted(list(v))[-5:] for m, v in years_by_metric.items()})
        print(f"→ falling back to latest overall year: {target_year} (ratio may be skipped)")

    present_any_metric = sorted([m for m, yrs in years_by_metric.items() if yrs])
    print("→ metrics with any data:", present_any_metric)
    desired_in_target = [m for m in desired_metrics if target_year in years_by_metric.get(m, set())]
    print("→ desired metrics available in target year:", desired_in_target)


    rows_acc = []

    for c in pd.read_csv(
        IN, sep=sep, dtype=str, engine="c",
        quoting=csv.QUOTE_NONE, on_bad_lines="skip",
        chunksize=CHUNK, encoding="utf-8", header=0, usecols=usecols_second_pass_callable,
        low_memory=False
    ):
        c.columns = [norm_header(x) for x in c.columns]
        need = {col_code, col_name, col_type, col_time, col_val}
        if not need.issubset(set(c.columns)):
            continue

        for col in need:
            c[col] = c[col].astype(str).str.strip().str.strip('"').str.strip()

        y = extract_years(c[col_time])
        c = c[y == target_year]
        if c.empty:
            continue

        desired_now = [m for m in desired_in_target if m in set(c[col_type].unique())]
        if not desired_now:
            continue
        c = c[c[col_type].isin(desired_now)]
        if c.empty:
            continue

        c['value_num'] = safe_to_numeric(c[col_val])

        out = c[[col_code, col_name, col_type, 'value_num']].rename(
            columns={col_code: 'code', col_name: 'name', col_type: 'metric'}
        )
        rows_acc.append(out)

    if not rows_acc:
        sys.exit("No data was found for the target year and the specified target indicators.")

    df = pd.concat(rows_acc, ignore_index=True)
    df = df.groupby(['code', 'name', 'metric'], as_index=False, dropna=False)['value_num'].max()

    present_metrics = sorted(df['metric'].unique().tolist())
    print("→ metrics present in target year:", present_metrics)

    piv = df.pivot_table(
        index=['code', 'name'], columns='metric',
        values='value_num', aggfunc='max'
    ).reset_index()

    rename_map = {k: v for k, v in TARGET.items() if k in piv.columns}
    piv = piv.rename(columns={"code": "territory_code", "name": "territory_name", **rename_map})

    if all(col in piv.columns for col in ("commuting_population", "resident_population")):
        piv["commuting_ratio"] = (piv["commuting_population"] / piv["resident_population"]).round(3)
    else:
        missing = [x for x in ("commuting_population","resident_population") if x not in piv.columns]
        print(f"⚠️  Skipping commuting_ratio because missing columns: {missing}")
        print("Tip: If RP_COM_DAY or RESPOP_AV is not available for the selected year, choose another year that includes both, or adjust the TARGET variable accordingly.")

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    piv.to_csv(OUT, index=False)
    print(f"✓ Saved: {OUT}  rows={len(piv)}  year={target_year}")
    with pd.option_context('display.max_columns', None):
        print(piv.head(12).to_string(index=False))

if __name__ == "__main__":
    main()
