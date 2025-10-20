#!/usr/bin/env python3
import os, csv
import pandas as pd
from istat_utils import sniff_sep, find_col

IN = "/home/tahmast/Projects/Tourism/Dataset/DCSS_ISTR_LAV_PEN_2 - Istruzione, lavoro e spostamenti per studio o lavoro - intero ds.csv"
OUT_DIR = "/home/tahmast/Projects/Tourism/data/curated"
os.makedirs(OUT_DIR, exist_ok=True)

NEEDED_CANDIDATES = {
    "code": ["ITTER107", "Codice territorio"],
    "name": ["Territorio", "Denominazione territorio", "Comune"],
    "metric": ["TIPO_DATO_CENS_POP", "Tipo dato"],
    "time": ["TIME", "Anno", "Seleziona periodo"],
    "value": ["Value", "Valore"],
    "sex": ["SEXISTAT1", "Sesso"],
    "age": ["ETA1", "Età"],
    "edu": ["TITOLO_STUDIO", "Grado di istruzione"],
    "work": ["FORZE_LAV", "Condizione professionale"],
    "dest": ["PROV_DEST_Z", "Luogo di destinazione"],
    "mot": ["MOTIVOPS1", "Motivo dello spostamento"],
    "cit": ["CITTADINANZA", "Cittadinanza"],
}

CHUNK = 100_000
MAX_SAMPLE = 2_000

def main():
    sep = sniff_sep(IN)
    head_df = pd.read_csv(IN, sep=sep, dtype=str, engine="python",
                          quoting=csv.QUOTE_NONE, on_bad_lines="skip",
                          nrows=2000, encoding="utf-8")
    # resolve column names
    resolved = {}
    for key, cands in NEEDED_CANDIDATES.items():
        try:
            resolved[key] = find_col(head_df, cands)
        except Exception:
            if key in ("sex","age","edu","work","dest","mot","cit"):
                continue
            else:
                raise
    usecols = sorted(set(resolved.values()))
    print("→ detected sep:", repr(sep))
    print("→ resolved columns:", resolved)

    uniq_metrics = set()
    counts = {}
    sample_rows = []
    total_rows = 0

    for chunk in pd.read_csv(IN, sep=sep, dtype=str, engine="python",
                             quoting=csv.QUOTE_NONE, on_bad_lines="skip",
                             usecols=usecols, chunksize=CHUNK, encoding="utf-8"):
        for c in chunk.columns:
            if chunk[c].dtype == object:
                chunk[c] = chunk[c].str.strip().str.strip('"').str.strip()

        mcol = resolved["metric"]
        vals = chunk[mcol].dropna().unique().tolist()
        uniq_metrics.update(vals)
        vc = chunk[mcol].value_counts()
        for k, v in vc.items():
            counts[k] = counts.get(k, 0) + int(v)

        if len(sample_rows) < MAX_SAMPLE:
            need = MAX_SAMPLE - len(sample_rows)
            sample_rows.extend(chunk.head(need).to_dict(orient="records"))

        total_rows += len(chunk)
        if total_rows > 2_000_000 and len(sample_rows) >= MAX_SAMPLE:
            break


    mlist_path = os.path.join(OUT_DIR, "istr_lav_metric_codes.txt")
    with open(mlist_path, "w", encoding="utf-8") as f:
        for m in sorted(uniq_metrics):
            f.write(str(m) + "\n")


    freq_path = os.path.join(OUT_DIR, "istr_lav_metric_freq.csv")
    pd.Series(counts, name="count").sort_values(ascending=False).to_csv(freq_path, header=True)


    sample_path = os.path.join(OUT_DIR, "istr_lav_sample.csv")
    pd.DataFrame(sample_rows).to_csv(sample_path, index=False)

    print(f"✓ metrics unique: {len(uniq_metrics)}  → {mlist_path}")
    print(f"✓ metric freq: {freq_path}")
    print(f"✓ sample rows: {len(sample_rows)}  → {sample_path}")

if __name__ == "__main__":
    main()
