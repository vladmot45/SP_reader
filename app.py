import re
from datetime import datetime
import io
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
    "Transport type": "Transport type",
    "Country": "Country",
    "Ex rate USD": "Ex rate USD",
    "Ex rate EUR": "Ex rate EUR",
    "EUR/USD": "EUR/USD",
    "Incoterms": "Incoterms",
}

HEADER_MARKERS = ["Contract", "Buyer", "Goods", "Price"]

FINAL_ORDER = [
    "Date", "Contract", "Transport type", "Country", "Buyer","Buyer abbreviation", "Protein", "Goods sold", "Contract status",
    "Delivery month", "Tonnes", "Price FCA", "Price Dap",  "Currency", "Ex rate USD", "Ex rate EUR", "EUR/USD", "Price FCA EUR",
"Price DAP EUR", "Total price FCA",
    "Total price DAP", "Total Price FCA EUR",  "Total Price DAP EUR", "Incoterms", "Uwagi",
    "Pick Up date", "Pick up quantity", "Pickup Total FCA", "Pickup Total DAP", "Pickup Total FCA EUR", "Pickup Total DAP EUR",

]



def norm(s) -> str:
    s = "" if s is None else str(s)
    return re.sub(r"\s+", " ", s.strip())

def first_non_empty_number(s: pd.Series):
    s = s.map(clean_number).dropna()
    return s.iloc[0] if len(s) else pd.NA


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
            cols.append(dt)          # month headers
        else:
            cols.append(norm(b))     # base headers
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

        # only month headers: 1st of month
        if dt.day != 1:
            continue

        # normalize to first-of-month (00:00)
        month_cols[col] = pd.Timestamp(dt.year, dt.month, 1)

    return month_cols


def first_non_empty_str(s: pd.Series):
    s = s.dropna().astype(str)
    s = s[s.str.strip() != ""]
    return s.iloc[0] if len(s) else pd.NA


def first_valid_datetime(s: pd.Series):
    s = pd.to_datetime(s, errors="coerce")
    s = s.dropna()
    return s.iloc[0] if len(s) else pd.NaT


def transform_excel_to_xlsx_bytes(file_obj) -> tuple[bytes, dict]:
    TARGET_SHEET = "SP"

    # --- verify sheet exists ---
    file_obj.seek(0)
    xl = pd.ExcelFile(file_obj, engine="openpyxl")
    if TARGET_SHEET not in xl.sheet_names:
        raise ValueError(f"Sheet '{TARGET_SHEET}' not found. Available sheets: {', '.join(xl.sheet_names)}")

    # --- preview to detect header row ---
    file_obj.seek(0)
    raw_preview = pd.read_excel(
        file_obj, sheet_name=TARGET_SHEET, header=None, nrows=400, engine="openpyxl"
    )
    header_row = find_header_row(raw_preview)

    file_obj.seek(0)

    if header_row == 0:
        df = pd.read_excel(file_obj, sheet_name=TARGET_SHEET, header=0, engine="openpyxl")
        df.columns = [c if isinstance(c, (pd.Timestamp, datetime)) else norm(c) for c in df.columns]
        data_df = df.copy()
    else:
        raw_full = pd.read_excel(file_obj, sheet_name=TARGET_SHEET, header=None, engine="openpyxl")
        cols = build_columns_from_two_rows(raw_full, header_row)
        raw_full.columns = cols
        data_df = raw_full.iloc[header_row + 1:].copy()


    # Resolve / rename base columns
    resolved = {}
    str_cols = [str(c) for c in data_df.columns]
    for excel_label, out_label in BASE_COLS.items():
        c_str = pick_col(str_cols, excel_label)
        if c_str is not None:
            real_col = next(col for col in data_df.columns if str(col) == c_str)
            resolved[real_col] = out_label

    base_df = data_df.rename(columns=resolved)

                # ---- Buyer abbreviation: extract text inside parentheses ----
    if "Buyer" in base_df.columns:
        def extract_abbr(x):
            if pd.isna(x):
                return pd.NA
            s = str(x)
            m = re.search(r"\(([^)]+)\)", s)
            if m:
                return m.group(1).strip()
            return pd.NA

        base_df.insert(
            base_df.columns.get_loc("Buyer") + 1,
            "Buyer abbreviation",
            base_df["Buyer"].apply(extract_abbr)
        )

    # ---- DATE CLEANING: drop invalid/blanks and KEEP datetime (Excel numeric) ----
    if "Date" in base_df.columns:
        s = (
            base_df["Date"]
            .astype(str)
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "none": pd.NA, "null": pd.NA})
        )
        parsed = pd.to_datetime(s, dayfirst=True, errors="coerce")
        base_df = base_df.loc[parsed.notna()].copy()
        base_df["Date"] = parsed.loc[parsed.notna()]  # keep datetime
    else:
        # you said: if no valid date, ignore row => if Date column missing, output nothing
        base_df = base_df.iloc[0:0].copy()

    # Default / create Transport type
    if "Transport type" in base_df.columns:
        base_df["Transport type"] = (
            base_df["Transport type"]
            .astype(str)
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            .fillna("Trucks")
        )
    else:
        base_df["Transport type"] = "Trucks"

    # Contract: blank -> N/A, and remove totals
    if "Contract" in base_df.columns:
        base_df["Contract"] = (
            base_df["Contract"]
            .astype(str)
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            .fillna("N/A")
        )
        base_df = base_df[~base_df["Contract"].str.lower().str.contains("total", na=False)]

    # Detect month columns
    month_cols = detect_month_columns(data_df.columns)
    if not month_cols:
        raise ValueError("No month columns found. Month headers must be dates (1st of month).")

    # CLOSED contracts with zero deliveries across all month columns
    closed_rows = pd.DataFrame()
    if "Contract status" in base_df.columns and "Contract" in base_df.columns:
        status = base_df["Contract status"].astype(str).str.lower().str.strip()
        is_closed = status.eq("closed")

        month_block = base_df[list(month_cols.keys())].apply(lambda col: col.map(clean_number))
        month_sum = month_block.fillna(0).sum(axis=1)

        closed_no_months = base_df[is_closed & (month_sum == 0)].copy()
        if not closed_no_months.empty:
            closed_rows = closed_no_months.copy()
            closed_rows["Delivery month"] = pd.NaT
            closed_rows["Tonnes"] = 0


    # Melt to long format
    id_vars = [c for c in FINAL_ORDER if c in base_df.columns and c not in ("Delivery month", "Tonnes")]

    long_df = base_df.melt(
        id_vars=id_vars,
        value_vars=list(month_cols.keys()),
        var_name="Delivery month",
        value_name="Tonnes",
    )

    long_df["Delivery month"] = long_df["Delivery month"].map(month_cols)  # "Mar 27"
    long_df["Tonnes"] = long_df["Tonnes"].apply(clean_number)
    long_df = long_df.dropna(subset=["Tonnes"])
    long_df = long_df[long_df["Tonnes"] != 0]

    for col in ["Goods sold", "Price FCA", "Price Dap","Ex rate USD", "Ex rate EUR", "EUR/USD"]:
        if col in long_df.columns:
            long_df[col] = long_df[col].apply(clean_number)

    if "Uwagi" in long_df.columns:
        long_df["Uwagi"] = long_df["Uwagi"].fillna("N/A")

    group_keys = [c for c in ["Contract", "Delivery month"] if c in long_df.columns]
    if not group_keys:
        raise ValueError("Cannot group output because Contract/Delivery month columns are missing after transform.")

    # Aggregation: Date must stay datetime; other fields as strings
    agg = {"Tonnes": "max"}
    if "Date" in long_df.columns:
        agg["Date"] = first_valid_datetime

    for col in ["Transport type", "Country", "Buyer", "Buyer abbreviation", "Protein", "Goods sold",
                "Contract status", "Currency","Incoterms", "Uwagi"]:
        if col in long_df.columns:
            agg[col] = first_non_empty_str
    
    for col in ["Price FCA", "Price Dap", "Ex rate USD", "Ex rate EUR", "EUR/USD"]:
        if col in long_df.columns:
            agg[col] = first_non_empty_number

    out = long_df.groupby(group_keys, as_index=False).agg(agg)

    # Show "Goods sold" only once per Contract (0 for other months)
    if "Goods sold" in out.columns and "Contract" in out.columns:
        out["Goods sold"] = out["Goods sold"].apply(clean_number).fillna(0)

    if "Delivery month" in out.columns:
        out = out.sort_values(["Contract", "Delivery month"], na_position="last")

    first_mask = ~out.duplicated(subset=["Contract"])
    out.loc[~first_mask, "Goods sold"] = 0


    # Append closed contracts that have no monthly tonnes
    if not closed_rows.empty:
        for c in out.columns:
            if c not in closed_rows.columns:
                closed_rows[c] = pd.NA
        closed_rows = closed_rows[out.columns]
        out = pd.concat([out, closed_rows], ignore_index=True, sort=False)

    # Final column order
    out = out[[c for c in FINAL_ORDER if c in out.columns]]

        # --- Total price columns ---
    # make sure operands are numeric (clean_number already handles strings like "1 234,56" etc.)
    if "Tonnes" in out.columns and "Price FCA" in out.columns:
        out["Tonnes"] = out["Tonnes"].apply(clean_number)
        out["Price FCA"] = out["Price FCA"].apply(clean_number)
        out["Total price FCA"] = out["Tonnes"] * out["Price FCA"]
    else:
        out["Total price FCA"] = pd.NA

    if "Tonnes" in out.columns and "Price Dap" in out.columns:
        out["Price Dap"] = out["Price Dap"].apply(clean_number)
        out["Total price DAP"] = out["Tonnes"] * out["Price Dap"]
    else:
        out["Total price DAP"] = pd.NA

        # --- Ensure FX + Currency are clean ---
    cur = out["Currency"].astype(str).str.strip().str.upper() if "Currency" in out.columns else ""

    def to_float64_nullable(series: pd.Series) -> pd.Series:
        # clean_number returns float or pd.NA; keep pd.NA safely with nullable Float64 dtype
        return series.map(clean_number).astype("Float64")

    # --- Total Price in EUR (based on Currency) ---
    # EUR -> keep total as-is
    # USD -> multiply by EUR/USD
    # PLN -> Divide by Ex rate EUR
    def to_eur(total_col: str) -> pd.Series:
        if total_col not in out.columns:
            return pd.Series(pd.NA, index=out.index, dtype="Float64")

        total = to_float64_nullable(out[total_col])

        fx_eurusd = to_float64_nullable(out["EUR/USD"]) if "EUR/USD" in out.columns else pd.Series(pd.NA, index=out.index, dtype="Float64")
        fx_eurpln = to_float64_nullable(out["Ex rate EUR"]) if "Ex rate EUR" in out.columns else pd.Series(pd.NA, index=out.index, dtype="Float64")

        res = pd.Series(pd.NA, index=out.index, dtype="Float64")

        res = res.mask(cur.eq("EUR"), total)
        res = res.mask(cur.eq("USD"), total * fx_eurusd)
        res = res.mask(cur.eq("PLN"), total / fx_eurpln)

        return res

    out["Total Price FCA EUR"] = to_eur("Total price FCA")
    out["Total Price DAP EUR"] = to_eur("Total price DAP")

        # --- Unit prices in EUR (based on Currency) ---
    def price_to_eur(price_col: str) -> pd.Series:
        if price_col not in out.columns:
            return pd.Series(pd.NA, index=out.index, dtype="Float64")

        price = to_float64_nullable(out[price_col])

        fx_eurusd = to_float64_nullable(out["EUR/USD"]) if "EUR/USD" in out.columns else pd.Series(pd.NA, index=out.index, dtype="Float64")
        fx_eurpln = to_float64_nullable(out["Ex rate EUR"]) if "Ex rate EUR" in out.columns else pd.Series(pd.NA, index=out.index, dtype="Float64")

        res = pd.Series(pd.NA, index=out.index, dtype="Float64")

        res = res.mask(cur.eq("EUR"), price)
        res = res.mask(cur.eq("USD"), price * fx_eurusd)
        res = res.mask(cur.eq("PLN"), price / fx_eurpln)

        return res

    out["Price FCA EUR"] = price_to_eur("Price FCA")
    out["Price DAP EUR"] = price_to_eur("Price Dap")

    


        # ----------------- Wagi total: Pick Up date + quantity -----------------
    WAGI_SHEET_WANTED = "Wagi total"  # adjust only if the name differs

    file_obj.seek(0)
    xl2 = pd.ExcelFile(file_obj, engine="openpyxl")

    # find sheet case-insensitively
    wagi_sheet = None
    for sn in xl2.sheet_names:
        if sn.strip().lower() == WAGI_SHEET_WANTED.strip().lower():
            wagi_sheet = sn
            break

    if wagi_sheet is not None:
        file_obj.seek(0)
        wagi_raw = pd.read_excel(file_obj, sheet_name=wagi_sheet, engine="openpyxl")

        # pick the 3 columns we need (robust matching)
        contract_col = None
        for c in wagi_raw.columns:
            if str(c).strip().lower() in {"№ контракта", "no kontrakta", "nr kontrakta", "contract"}:
                contract_col = c
                break
        if contract_col is None:
            # fallback: partial match
            for c in wagi_raw.columns:
                if "контракт" in str(c).strip().lower():
                    contract_col = c
                    break

        date_col = None
        for c in wagi_raw.columns:
            if str(c).strip().lower() in {"data wz", "pick up date"}:
                date_col = c
                break

        qty_col = None
        for c in wagi_raw.columns:
            if str(c).strip().lower() in {"q-ty", "qty", "quantity", "pick up quantity"}:
                qty_col = c
                break

        if contract_col is not None and date_col is not None and qty_col is not None:
            wagi = wagi_raw[[contract_col, date_col, qty_col]].copy()
            wagi.columns = ["Contract", "Pick Up date", "Pick up quantity"]

            # normalize keys + values
            wagi["Contract"] = wagi["Contract"].astype(str).str.strip()
            wagi["Pick Up date"] = pd.to_datetime(wagi["Pick Up date"], dayfirst=True, errors="coerce")
            wagi["Pick up quantity"] = wagi["Pick up quantity"].apply(clean_number)

            wagi = wagi.dropna(subset=["Contract", "Pick Up date", "Pick up quantity"])
            wagi["Pick up quantity"] = wagi["Pick up quantity"] / 1000.0  # 25160 -> 25.160

        

            # attach pickups ONLY to the first row per contract (prevents duplicating pickups for every delivery month)
            if "Contract" in out.columns and len(out) > 0:
                first_mask2 = ~out.duplicated(subset=["Contract"])
                base_first = out.loc[first_mask2].copy()
                base_rest = out.loc[~first_mask2].copy()

                merged_first = base_first.merge(wagi, on="Contract", how="left", suffixes=("", "_w"))

                    # overwrite base cols with the merged ones, then drop the _w columns
                for col in ["Pick Up date", "Pick up quantity"]:                 
                    wcol = f"{col}_w"
                    if wcol in merged_first.columns:
                        merged_first[col] = merged_first[wcol]
                        merged_first = merged_first.drop(columns=[wcol])

                # merge creates Pick Up date / quantity from wagi; keep them
                out = pd.concat([merged_first, base_rest], ignore_index=True, sort=False)

                # optional: sort nicely
                sort_cols = [c for c in ["Contract", "Delivery month", "Pick Up date"] if c in out.columns]
                if sort_cols:
                    out = out.sort_values(sort_cols, na_position="last").reset_index(drop=True)
        else:
            # sheet exists but missing needed columns -> still produce output without pickups
            pass
    # ----------------------------------------------------------------------
    # --- Pickup totals = Pick up quantity * prices (AFTER Wagi merge) ---
    def f64(series_name: str) -> pd.Series:
        if series_name not in out.columns:
            return pd.Series(pd.NA, index=out.index, dtype="Float64")
        return out[series_name].map(clean_number).astype("Float64")

    pu_qty = f64("Pick up quantity")

    out["Pickup Total FCA"] = pu_qty * f64("Price FCA")
    out["Pickup Total DAP"] = pu_qty * f64("Price Dap")
    out["Pickup Total FCA EUR"] = pu_qty * f64("Price FCA EUR")
    out["Pickup Total DAP EUR"] = pu_qty * f64("Price DAP EUR")

    # Final column order (do this at the end so new columns are kept)
    out = out[[c for c in FINAL_ORDER if c in out.columns]]



    # ---- WRITE XLSX (no locale separator issues), keep Date as numeric with display format ----
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="Output")
        ws = writer.sheets["Output"]

        if "Date" in out.columns:
            date_col_idx = out.columns.get_loc("Date") + 1  # Excel is 1-based
            for r in range(2, ws.max_row + 1):
                cell = ws.cell(row=r, column=date_col_idx)
                if isinstance(cell.value, (datetime, pd.Timestamp)):
                    cell.number_format = "DD/MM/YYYY"
            # Format Delivery month as "Apr 26" but keep it numeric
        if "Delivery month" in out.columns:
            dm_col_idx = out.columns.get_loc("Delivery month") + 1
            for r in range(2, ws.max_row + 1):
                cell = ws.cell(row=r, column=dm_col_idx)
                if isinstance(cell.value, (datetime, pd.Timestamp)):
                    cell.number_format = "mmm yy"


    xlsx_bytes = buffer.getvalue()

    meta = {
        "header_row": header_row,
        "month_cols_detected": [str(k) for k in month_cols.keys()],
        "rows_out": int(out.shape[0]),
    }
    return xlsx_bytes, meta


st.title("SP → Pivotable data converter (XLSX output)")

uploaded = st.file_uploader("Upload SP file (.xlsx/.xlsm/.xls)", type=["xlsx", "xlsm", "xls"])
if uploaded:
    try:
        xlsx_bytes, meta = transform_excel_to_xlsx_bytes(uploaded)

        st.success(f"Done. Output rows: {meta['rows_out']}")
        st.caption(f"Header row used: {meta['header_row']}")
        st.caption(
            f"Month columns detected: {', '.join(meta['month_cols_detected'][:10])}"
            + (" ..." if len(meta['month_cols_detected']) > 10 else "")
        )

        from pathlib import Path
        output_name = Path(uploaded.name).stem + "_output.xlsx"

        st.download_button(
            "Download Excel",
            data=xlsx_bytes,
            file_name=output_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        st.error(str(e))
