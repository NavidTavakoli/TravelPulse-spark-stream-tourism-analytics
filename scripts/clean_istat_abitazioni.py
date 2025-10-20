#!/usr/bin/env python3
import os
import pandas as pd
from istat_utils import read_istat_auto, find_col, coerce_num_series, is_macro_territory


IN = "/home/tahmast/Projects/Tourism/Dataset/DCSS_ABITAZIONI - Abitazioni - intero ds.csv"
OUT = "/home/tahmast/Projects/Tourism/data/curated/istat_homes.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)

TARGET_CODES = {
    "NUM_DW_AV": "dwellings_total",
    "NUM_OCC_DW_AV": "dwellings_occupied",
}

def main():
    df = read_istat_auto(IN)
    col_code = find_col(df, ['ITTER107','Codice territorio'])
    col_name = find_col(df, ['Territorio','Denominazione territorio'])
    col_type = find_col(df, ['TIPO_DATO_CENS_POP','Tipo dato'])
    col_time = find_col(df, ['TIME','Anno'])
    col_val  = find_col(df, ['Value','Valore'])


    mask = df[col_type].isin(TARGET_CODES.keys())
    df = df[mask].copy()

    year = pd.to_numeric(df[col_time], errors='coerce').max()
    df = df[df[col_time] == str(int(year))].copy()

    df['value_num'] = coerce_num_series(df[col_val]).fillna(0)
    df = df[~is_macro_territory(df[col_code])].copy()


    piv = df.pivot_table(index=[col_code, col_name], columns=col_type, values='value_num', aggfunc='max').reset_index()
    piv = piv.rename(columns={k:v for k,v in TARGET_CODES.items() if k in piv.columns})

    if 'dwellings_total' in piv.columns and 'dwellings_occupied' in piv.columns:
        piv['occupied_share'] = (piv['dwellings_occupied'] / piv['dwellings_total']).clip(upper=1.0)

    piv = piv.rename(columns={col_code:'territory_code', col_name:'territory_name'})
    piv.to_csv(OUT, index=False)
    print(f"✓ Saved: {OUT} (rows={len(piv)}, year={int(year)})")
    print(piv.head(5).to_string(index=False))

if __name__ == "__main__":
    main()
