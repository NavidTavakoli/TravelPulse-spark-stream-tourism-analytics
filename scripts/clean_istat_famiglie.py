#!/usr/bin/env python3
import os, pandas as pd, numpy as np
from istat_utils import read_istat_auto, find_col, coerce_num_series, is_macro_territory

IN = "/home/tahmast/Projects/Tourism/Dataset/DCSS_FAMIGLIE - Famiglie per numero di componenti - intero ds.csv"
OUT = "/home/tahmast/Projects/Tourism/data/curated/istat_families.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)

def parse_component_count(s: pd.Series) -> pd.Series:

    x = s.str.extract(r'(\d+)').astype(float)

    x[s.str.contains('piu', case=False, na=False)] = 4.5
    return x[0]

def main():
    df = read_istat_auto(IN)
    col_code = find_col(df, ['ITTER107','Codice territorio'])
    col_name = find_col(df, ['Territorio','Denominazione territorio'])
    col_type = find_col(df, ['TIPO_DATO_CENS_POP','Tipo dato'])  # انتظار NFPHH
    col_comp = find_col(df, ['Numero di componenti','NUMEROSITA_A'])
    col_time = find_col(df, ['TIME','Anno'])
    col_val  = find_col(df, ['Value','Valore'])


    df = df[df[col_type].str.contains(r'\bNFPHH\b', case=False, na=False)].copy()


    year = pd.to_numeric(df[col_time], errors='coerce').max()
    df = df[df[col_time] == str(int(year))].copy()

    df = df[~is_macro_territory(df[col_code])].copy()


    df['families_count'] = coerce_num_series(df[col_val]).fillna(0)


    df['comp_n'] = parse_component_count(df[col_comp])


    g = df.groupby([col_code, col_name], as_index=False).agg(
        families_total=('families_count','sum'),

        families_3plus=('families_count', lambda s: s[df.loc[s.index,'comp_n']>=3].sum()),

        avg_family_size_from_istat=('families_count', lambda s: (
            (df.loc[s.index,'families_count'] * df.loc[s.index,'comp_n']).sum() / max(s.sum(),1)
        ))
    )
    g['families_3plus_share'] = (g['families_3plus'] / g['families_total']).where(g['families_total']>0)

    g = g.rename(columns={col_code:'territory_code', col_name:'territory_name'})
    g[['territory_code','territory_name','families_total','families_3plus_share','avg_family_size_from_istat']] \
        .to_csv(OUT, index=False)
    print(f"✓ Saved: {OUT} (rows={len(g)}, year={int(year)})")
    print(g.head(5).to_string(index=False))

if __name__ == "__main__":
    main()
