import re
from datetime import datetime
import pandas as pd
import streamlit as st


BASE_COLS = {
    "Date": "Date",
    "Contract": "Contract",
    "Buyer": "Buyer",
    "Protein": "Protein",
    "Prot": "Protein",
    "Goods sold": "Goods sold",
    "Contract status": "Contract status",
    "Contr status": "Contract status",
    "Price FCA": "Price FCA",
    "Price DAP": "Price Dap",
    "Price Dap": "Price Dap",
    "Uwagi": "Uwagi",
    "UWAGI": "Uwagi",
    "Currency": "Currency",
}

HEADER_MARKERS = ["Contract", "Buyer", "Goods", "Price"]

FINAL_ORDER = [
    "Date", "Contract", "Buyer", "Protein", "Goods sold", "Contract status",
    "Delivery month", "Tonnes", "Price FCA", "Price Dap", "Currency", "Uwagi"
]


def norm(s) -> str:
    s = "" if s is None else str(s)
    return re.sub(r"\s+", " ", s.strip())


def clean_number(x):
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    if s == "":
        return pd.NA

    s = re.sub(r"[€$]", "", s)
    s = re.sub(r"\b(PLN|EUR|USD)\b", "", s, flags=re.IGNORECASE)
    s = s.replace(" ", "")

    if "," in s and "." in s:
        s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")

    s = re.sub(r"[^0-9.\-]", "", s)
    if s in ("", "-", ".", "-."):
        return pd.NA

    try:
        return float(s)
    except ValueError:
        return pd.NA


def find_header_row(preview: pd.DataFrame) -> int:
    best_i, best_score = 0, -1
    for i in range(min(len(preview), 400)):
        row = " | ".join(norm(v) for v in preview.iloc[i].tolist())
        score = sum(1 for m in HEADER_MARKERS if m.lower() in row.lower())
        if score > best_score:
            best_score = score
            best_i = i
    if best_score <= 0:
        raise ValueError("Could not detect header row.")
    return best_i


def pick_col(df_cols, wanted_label):
    wanted = wanted_label.lower().strip()
    for c in df_cols:
        if str(c).strip().lower() == wanted:
            return c
    for c in df_cols:
        if wanted in str(c).strip().lower():
            return c
    return None


def to_dt_if_possible(x):
    if isinstance(x, (pd.Timestamp, datetime)):
        return pd.to_datetime(x)
    try:
        return pd.to_datetime(x, dayfirst=True, errors="raise")
    except Exception:
        return None


def build_columns_from_two_rows(raw: pd.DataFrame, header_row: int):
    top = raw.iloc[header_row - 1].tolist()
    bottom = raw.iloc[header_row].tolist()
    cols = []
    for t, b in zip(top, bottom):
        dt = to_dt_if_possible(t)
        if dt is not None and dt.day == 1:
            cols.append(dt)          # keep month headers as datetime
        else:
            cols.append(norm(b))     # base headers like Contract, Buyer
    return cols


def detect_month_columns(columns):
    month_cols = {}
    for col in columns:
        low = str(col).strip().lower()
        if "total" in low or "sum" in low or "razem" in low:
            continue

        dt = None
        if isinstance(col, (pd.Timestamp, datetime)):
            dt = pd.to_datetime(col)
        else:
            try:
                dt = pd.to_datetime(col, dayfirst=True, errors="raise")
            except Exception:
                continue

        month_cols[col] = dt.strftime("%b %Y")  # e.g. Apr 2026
    return month_cols


def transform_excel_to_csv_bytes(file_obj) -> tuple[bytes, dict]:
    raw_preview = pd.read_excel(file_obj, sheet_name=0, header=None, nrows=400, engine="openpyxl")
    header_row = find_header_row(raw_preview)

    # Rewind file for subsequent reads
    file_obj.seek(0)

    if header_row == 0:
        df = pd.read_excel(file_obj, sheet_name=0, header=0, engine="openpyxl")
        df.columns = [c if isinstance(c, (pd.Timestamp, datetime)) else norm(c) for c in df.columns]
        data_df = df.copy()
    else:
        file_obj.seek(0)
        raw_full = pd.read_excel(file_obj, sheet_name=0, header=None, engine="openpyxl")
        cols = build_columns_from_two_rows(raw_full, header_row)
        raw_full.columns = cols
        data_df = raw_full.iloc[header_row + 1:].copy()

    resolved = {}
    str_cols = [str(c) for c in data_df.columns]
    for excel_label, out_label in BASE_COLS.items():
        c_str = pick_col(str_cols, excel_label)
        if c_str is not None:
            real_col = next(col for col in data_df.columns if str(col) == c_str)
            resolved[real_col] = out_label

    base_df = data_df.rename(columns=resolved)

    if "Contract" in base_df.columns:
        base_df = base_df[base_df["Contract"].notna()]
        base_df = base_df[~base_df["Contract"].astype(str).str.lower().str.contains("total", na=False)]

    month_cols = detect_month_columns(data_df.columns)
    if not month_cols:
        raise ValueError("No month columns found. Month headers must be dates (1st of month).")

    id_vars = [c for c in FINAL_ORDER if c in base_df.columns and c not in ("Delivery month", "Tonnes")]
    long_df = base_df.melt(
        id_vars=id_vars,
        value_vars=list(month_cols.keys()),
        var_name="Delivery month",
        value_name="Tonnes",
    )

    long_df["Delivery month"] = long_df["Delivery month"].map(month_cols)
    long_df["Tonnes"] = long_df["Tonnes"].apply(clean_number)
    long_df = long_df.dropna(subset=["Tonnes"])
    long_df = long_df[long_df["Tonnes"] != 0]

    for col in ["Goods sold", "Price FCA", "Price Dap"]:
        if col in long_df.columns:
            long_df[col] = long_df[col].apply(clean_number)

    if "Date" in long_df.columns:
        long_df["Date"] = pd.to_datetime(long_df["Date"], errors="coerce").dt.strftime("%d.%m.%Y")

    if "Uwagi" in long_df.columns:
        long_df["Uwagi"] = long_df["Uwagi"].fillna("N/A")

    key_cols = [c for c in FINAL_ORDER if c in long_df.columns and c != "Tonnes"]
    out = long_df.groupby(key_cols, as_index=False)["Tonnes"].max()
    out = out[[c for c in FINAL_ORDER if c in out.columns]]

    csv_bytes = out.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    meta = {
        "header_row": header_row,
        "month_cols_detected": [str(k) for k in month_cols.keys()],
        "rows_out": int(out.shape[0]),
    }
    return csv_bytes, meta


st.title("Excel → CSV converter")

uploaded = st.file_uploader("Upload Excel (.xlsx/.xlsm/.xls)", type=["xlsx", "xlsm", "xls"])
if uploaded:
    try:
        csv_bytes, meta = transform_excel_to_csv_bytes(uploaded)
        st.success(f"Done. Output rows: {meta['rows_out']}")
        st.caption(f"Header row used: {meta['header_row']}")
        st.caption(f"Month columns detected: {', '.join(meta['month_cols_detected'][:10])}"
                   + (" ..." if len(meta['month_cols_detected']) > 10 else ""))

        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name="output.csv",
            mime="text/csv",
        )
    except Exception as e:
        st.error(str(e))
