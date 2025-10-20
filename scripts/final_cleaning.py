import pandas as pd
from pathlib import Path


IN_PATH  = "/home/tahmast/Projects/Tourism/data/curated/cities_it_enriched_filled.csv"
OUT_PATH = "/home/tahmast/Projects/Tourism/data/curated/cities_it_enriched_cleaned.csv"
REPORT_DIR = "/home/tahmast/Projects/Tourism/data/curated/_reports"

INT_COLS = [
    "households",
    "territory_code",
    "year",
    "dwellings_total",
    "dwellings_occupied",
    "families_total",
    "resident_population",
    "commuting_population",
]

def main():
    Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(IN_PATH)

    df_raw = df.copy()

    any_decimals = False

    for col in INT_COLS:
        if col not in df.columns:
            print(f"⚠️ The column '{col}' does not exist in the file and has been skipped.")
            continue


        s_num = pd.to_numeric(df[col], errors="coerce")

        decimals_mask = s_num.notna() & (s_num % 1 != 0)
        n_decimals = int(decimals_mask.sum())
        if n_decimals > 0:
            any_decimals = True
            print(f"ℹ️  {col}: {n_decimals} decimal values → rounded to the nearest integer.")
        n_nonnum = int(s_num.isna().sum())
        if n_nonnum > 0:
            print(f"ℹ️  {col}: {n_nonnum} non-numeric or empty values → replaced with 0.")


        df[col] = s_num.fillna(0).round(0).astype("int64")


    if any_decimals:

        mask_any_decimal = None
        for col in INT_COLS:
            if col in df_raw.columns:
                s_num = pd.to_numeric(df_raw[col], errors="coerce")
                m = s_num.notna() & (s_num % 1 != 0)
                mask_any_decimal = m if mask_any_decimal is None else (mask_any_decimal | m)

        decimals_df = df_raw.loc[mask_any_decimal] if mask_any_decimal is not None else pd.DataFrame()
        if not decimals_df.empty:
            dec_report = f"{REPORT_DIR}/rows_with_decimals_before_rounding.csv"
            decimals_df.to_csv(dec_report, index=False)
            print(f"📝 Report of rows containing decimal values (before rounding): {dec_report}")


    for c in INT_COLS:
        if c in df.columns and not pd.api.types.is_integer_dtype(df[c]):
            raise TypeError(f"{c} is still not an integer!")

    df.to_csv(OUT_PATH, index=False, encoding="utf-8")
    print(f"✅ Completed: No rows were removed. File saved at: {OUT_PATH}")

if __name__ == "__main__":
    main()
