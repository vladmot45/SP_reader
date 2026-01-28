import re
from datetime import datetime
import io
from pathlib import Path

import pandas as pd
import streamlit as st


# ---------------- CONFIG ----------------
TARGET_SHEET = "SP"
WAGI_SHEET_WANTED = "Wagi total"

BASE_COLS = {
    "Season": "Season",
    "SEASON": "Season",

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

    # NEW
    "Trader": "Trader",
    "TRADER": "Trader",
}

HEADER_MARKERS = ["Contract", "Buyer", "Goods", "Price"]

FINAL_ORDER = [
    "Season", "Trader",
    "Date", "Contract", "Transport type", "Country", "Buyer", "Buyer abbreviation", "Protein",
    "Goods sold", "Contract status", "Delivery month", "Tonnes",
    "Price FCA", "Price Dap", "Currency", "Ex rate USD", "Ex rate EUR", "EUR/USD",
    "Price FCA EUR", "Price DAP EUR",
    "Total price FCA", "Total price DAP", "Total Price FCA EUR", "Total Price DAP EUR",
    "Incoterms", "Uwagi",
    "Pick Up date", "Pick up quantity",
    "Pickup Total FCA", "Pickup Total DAP", "Pickup Total FCA EUR", "Pickup Total DAP EUR",
]


# ---------------- HELPERS ----------------
def norm(s) -> str:
    s = "" if s is None else str(s)
    return re.sub(r"\s+", " ", s.strip())


def norm_key(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x)
    s = s.replace("\u00A0", " ")
    s = s.replace("\u200b", "")
    s = s.replace("\ufeff", "")
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s.upper()


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


def first_non_empty_number(s: pd.Series):
    s = s.map(clean_number).dropna()
    return s.iloc[0] if len(s) else pd.NA


def first_non_empty_str(s: pd.Series):
    s = s.dropna().astype(str)
    s = s[s.str.strip() != ""]
    return s.iloc[0] if len(s) else pd.NA


def first_valid_datetime(s: pd.Series):
    s = pd.to_datetime(s, errors="coerce")
    s = s.dropna()
    return s.iloc[0] if len(s) else pd.NaT


def join_unique_non_empty_str(s: pd.Series, sep="/"):
    """
    Join unique non-empty strings (first-seen order), e.g. Season1/Season2.
    """
    if s is None or len(s) == 0:
        return pd.NA
    s2 = s.dropna().astype(str).map(lambda x: x.strip())
    s2 = s2[s2 != ""]
    if len(s2) == 0:
        return pd.NA
    uniq = list(dict.fromkeys(s2.tolist()))
    return sep.join(uniq) if uniq else pd.NA


def sum_non_empty_number(s: pd.Series):
    """
    Sum numeric values (after clean_number). Returns pd.NA if nothing to sum.
    """
    if s is None or len(s) == 0:
        return pd.NA
    vals = s.map(clean_number).dropna()
    if len(vals) == 0:
        return pd.NA
    return float(vals.sum())


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
            cols.append(dt)  # month headers
        else:
            bt = norm(b)
            tt = norm(t)
            cols.append(bt if bt != "" else tt)

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
            dt = pd.to_datetime(str(col), dayfirst=True, errors="coerce")
            if pd.isna(dt):
                dt = pd.to_datetime(str(col), format="%b-%y", errors="coerce")

        if pd.isna(dt):
            continue

        month_cols[col] = pd.Timestamp(dt.year, dt.month, 1)

    return month_cols


def to_float64_nullable(series: pd.Series) -> pd.Series:
    return series.map(clean_number).astype("Float64")


def coalesce_columns_rowwise(df: pd.DataFrame, cols: list) -> pd.Series:
    """
    Return first non-empty value across given columns per row.
    """
    if not cols:
        return pd.Series(pd.NA, index=df.index)

    tmp = df[cols].copy()
    for c in cols:
        tmp[c] = (
            tmp[c]
            .astype("string")
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "none": pd.NA, "null": pd.NA})
        )

    out = tmp.iloc[:, 0]
    for c in tmp.columns[1:]:
        out = out.fillna(tmp[c])
    return out


# ---------------- MAIN TRANSFORM ----------------
def transform_excel_to_xlsx_bytes(file_obj) -> tuple[bytes, dict]:
    # ---- check SP sheet exists ----
    file_obj.seek(0)
    xl = pd.ExcelFile(file_obj, engine="openpyxl")
    if TARGET_SHEET not in xl.sheet_names:
        raise ValueError(f"Sheet '{TARGET_SHEET}' not found. Available sheets: {', '.join(xl.sheet_names)}")

    # ---- detect header row ----
    file_obj.seek(0)
    raw_preview = pd.read_excel(file_obj, sheet_name=TARGET_SHEET, header=None, nrows=400, engine="openpyxl")
    header_row = find_header_row(raw_preview)

    # ---- read SP ----
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

    # ---- rename base columns ----
    resolved = {}
    str_cols = [str(c) for c in data_df.columns]
    for excel_label, out_label in BASE_COLS.items():
        c_str = pick_col(str_cols, excel_label)
        if c_str is not None:
            real_col = next(col for col in data_df.columns if str(col) == c_str)
            resolved[real_col] = out_label

    base_df = data_df.rename(columns=resolved)

    # ---- Season: robust detection + coalesce ----
    season_candidates = []
    for c in data_df.columns:
        k = norm_key(c)
        if k == "SEASON" or "SEASON" in k:
            season_candidates.append(c)

    if "Season" in base_df.columns:
        season_cols_in_base = [c for c in base_df.columns if norm_key(c) == "SEASON"]
        season_src_cols = list(dict.fromkeys(season_cols_in_base + ["Season"] + [c for c in season_candidates if c in base_df.columns]))
        base_df["Season"] = coalesce_columns_rowwise(base_df, [c for c in season_src_cols if c in base_df.columns])
    else:
        candidates_in_base = [c for c in season_candidates if c in base_df.columns]
        if candidates_in_base:
            base_df["Season"] = coalesce_columns_rowwise(base_df, candidates_in_base)
        else:
            fallback = [c for c in base_df.columns if "SEASON" in norm_key(c)]
            base_df["Season"] = coalesce_columns_rowwise(base_df, fallback) if fallback else pd.NA

    # ---- Trader: robust detection ----
    trader_candidates = []
    for c in data_df.columns:
        k = norm_key(c)
        if k == "TRADER" or "TRADER" in k:
            trader_candidates.append(c)

    if "Trader" in base_df.columns:
        trader_cols_in_base = [c for c in base_df.columns if norm_key(c) == "TRADER"]
        trader_src_cols = list(dict.fromkeys(trader_cols_in_base + ["Trader"] + [c for c in trader_candidates if c in base_df.columns]))
        base_df["Trader"] = coalesce_columns_rowwise(base_df, [c for c in trader_src_cols if c in base_df.columns])
    else:
        candidates_in_base = [c for c in trader_candidates if c in base_df.columns]
        base_df["Trader"] = coalesce_columns_rowwise(base_df, candidates_in_base) if candidates_in_base else pd.NA

    # Buyer abbreviation
    if "Buyer" in base_df.columns:
        def extract_abbr(x):
            if pd.isna(x):
                return pd.NA
            m = re.search(r"\(([^)]+)\)", str(x))
            return m.group(1).strip() if m else pd.NA

        base_df.insert(
            base_df.columns.get_loc("Buyer") + 1,
            "Buyer abbreviation",
            base_df["Buyer"].apply(extract_abbr),
        )

    # Date cleaning
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
        base_df["Date"] = parsed.loc[parsed.notna()]
    else:
        base_df = base_df.iloc[0:0].copy()

    # Transport type default
    if "Transport type" in base_df.columns:
        base_df["Transport type"] = (
            base_df["Transport type"].astype(str).str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            .fillna("Trucks")
        )
    else:
        base_df["Transport type"] = "Trucks"

    # Contract cleaning
    if "Contract" in base_df.columns:
        base_df["Contract"] = (
            base_df["Contract"].astype(str).str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            .fillna("N/A")
        )
        base_df = base_df[~base_df["Contract"].str.lower().str.contains("total", na=False)]
    else:
        base_df["Contract"] = "N/A"

    # Detect month columns
    month_cols = detect_month_columns(data_df.columns)
    if not month_cols:
        raise ValueError("No month columns found. Month headers must be dates (1st of month).")

    # Melt to long
    id_vars = [c for c in base_df.columns if c not in month_cols.keys()]
    long_df = base_df.melt(
        id_vars=id_vars,
        value_vars=list(month_cols.keys()),
        var_name="Delivery month",
        value_name="Tonnes",
    )
    long_df["Delivery month"] = long_df["Delivery month"].map(month_cols)
    long_df["Tonnes"] = long_df["Tonnes"].apply(clean_number)

    # Keep only delivered rows (non-zero tonnes)
    delivered = long_df.dropna(subset=["Tonnes"])
    delivered = delivered[delivered["Tonnes"] != 0].copy()

    # Clean numeric fields in delivered
    for col in ["Goods sold", "Price FCA", "Price Dap", "Ex rate USD", "Ex rate EUR", "EUR/USD"]:
        if col in delivered.columns:
            delivered[col] = delivered[col].apply(clean_number)

    if "Uwagi" in delivered.columns:
        delivered["Uwagi"] = delivered["Uwagi"].fillna("N/A")

    # Group/aggregate delivered rows
    group_keys = [c for c in ["Contract", "Delivery month"] if c in delivered.columns]
    if not group_keys:
        raise ValueError("Cannot group output because Contract/Delivery month columns are missing.")

    agg = {"Tonnes": "max"}
    if "Date" in delivered.columns:
        agg["Date"] = first_valid_datetime

    for col in [
        "Transport type", "Country", "Buyer", "Buyer abbreviation", "Protein",
        "Contract status", "Currency", "Incoterms", "Uwagi",
        "Season", "Trader"
    ]:
        if col in delivered.columns:
            agg[col] = first_non_empty_str

    for col in ["Goods sold", "Price FCA", "Price Dap", "Ex rate USD", "Ex rate EUR", "EUR/USD"]:
        if col in delivered.columns:
            agg[col] = first_non_empty_number

    out = delivered.groupby(group_keys, as_index=False).agg(agg)

    # --- totals (Tonnes * unit price) ---
    out["Tonnes"] = out["Tonnes"].apply(clean_number)

    if "Price FCA" in out.columns:
        out["Price FCA"] = out["Price FCA"].apply(clean_number)
        out["Total price FCA"] = out["Tonnes"] * out["Price FCA"]
    else:
        out["Total price FCA"] = pd.NA

    if "Price Dap" in out.columns:
        out["Price Dap"] = out["Price Dap"].apply(clean_number)
        out["Total price DAP"] = out["Tonnes"] * out["Price Dap"]
    else:
        out["Total price DAP"] = pd.NA

    # --- FX conversions ---
    cur = out["Currency"].astype(str).str.strip().str.upper() if "Currency" in out.columns else pd.Series("", index=out.index)

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

    # ----------------- WAGI MERGE + ENSURE MISSING CONTRACTS EXIST -----------------
    file_obj.seek(0)
    xl2 = pd.ExcelFile(file_obj, engine="openpyxl")

    wagi_sheet = None
    for sn in xl2.sheet_names:
        if sn.strip().lower() == WAGI_SHEET_WANTED.strip().lower():
            wagi_sheet = sn
            break

    if wagi_sheet is not None:
        file_obj.seek(0)
        wagi_raw = pd.read_excel(file_obj, sheet_name=wagi_sheet, engine="openpyxl")

        # contract col
        contract_col = None
        for c in wagi_raw.columns:
            cl = str(c).strip().lower()
            if cl in {"№ контракта", "no kontrakta", "nr kontrakta", "contract"} or "контракт" in cl:
                contract_col = c
                break

        # pick up date col
        date_col = None
        for c in wagi_raw.columns:
            if "data wz" in str(c).strip().lower():
                date_col = c
                break

        # qty col
        qty_col = None
        for c in wagi_raw.columns:
            cl = str(c).strip().lower()
            if "q-ty" in cl or "qty" in cl:
                qty_col = c
                break

        if contract_col and date_col and qty_col:
            wagi = wagi_raw[[contract_col, date_col, qty_col]].copy()
            wagi.columns = ["Contract", "Pick Up date", "Pick up quantity"]

            wagi["Contract_key"] = wagi["Contract"].apply(norm_key)
            wagi["Pick Up date"] = pd.to_datetime(wagi["Pick Up date"], dayfirst=True, errors="coerce")
            wagi["Pick up quantity"] = wagi["Pick up quantity"].apply(clean_number) / 1000.0
            wagi = wagi.dropna(subset=["Contract_key", "Pick Up date", "Pick up quantity"])

            # Ensure contracts that exist in Wagi also exist in out (even if no tonnes rows)
            out_keys = set(out["Contract"].apply(norm_key)) if "Contract" in out.columns else set()
            wagi_keys = set(wagi["Contract_key"])
            missing_keys = wagi_keys - out_keys

            if missing_keys:
                # Build contract-level placeholders from SP base_df
                b = base_df.copy()
                b["Contract_key"] = b["Contract"].apply(norm_key)
                b = b[b["Contract_key"].isin(missing_keys)].copy()

                if not b.empty:
                    def agg_or_na(col, fn):
                        return fn if col in b.columns else (lambda s: pd.NA)

                    placeholders = b.groupby("Contract_key", as_index=False).agg({
                        "Contract": "first",
                        "Date": agg_or_na("Date", first_valid_datetime),
                        "Season": agg_or_na("Season", first_non_empty_str),
                        "Trader": agg_or_na("Trader", first_non_empty_str),
                        "Transport type": agg_or_na("Transport type", first_non_empty_str),
                        "Country": agg_or_na("Country", first_non_empty_str),
                        "Buyer": agg_or_na("Buyer", first_non_empty_str),
                        "Buyer abbreviation": agg_or_na("Buyer abbreviation", first_non_empty_str),
                        "Protein": agg_or_na("Protein", first_non_empty_str),
                        "Goods sold": agg_or_na("Goods sold", first_non_empty_number),
                        "Contract status": agg_or_na("Contract status", first_non_empty_str),
                        "Currency": agg_or_na("Currency", first_non_empty_str),
                        "Price FCA": agg_or_na("Price FCA", first_non_empty_number),
                        "Price Dap": agg_or_na("Price Dap", first_non_empty_number),
                        "Ex rate USD": agg_or_na("Ex rate USD", first_non_empty_number),
                        "Ex rate EUR": agg_or_na("Ex rate EUR", first_non_empty_number),
                        "EUR/USD": agg_or_na("EUR/USD", first_non_empty_number),
                        "Incoterms": agg_or_na("Incoterms", first_non_empty_str),
                        "Uwagi": agg_or_na("Uwagi", first_non_empty_str),
                    })

                    placeholders["Delivery month"] = pd.NaT
                    placeholders["Tonnes"] = 0

                    out = pd.concat([out, placeholders.drop(columns=["Contract_key"])], ignore_index=True, sort=False)

                    # recompute totals/fx for newly appended rows
                    cur2 = out["Currency"].astype(str).str.strip().str.upper() if "Currency" in out.columns else pd.Series("", index=out.index)

                    def to_eur_any(df, colname):
                        if colname not in df.columns:
                            return pd.Series(pd.NA, index=df.index, dtype="Float64")
                        total = to_float64_nullable(df[colname])
                        fx_eurusd = to_float64_nullable(df["EUR/USD"]) if "EUR/USD" in df.columns else pd.Series(pd.NA, index=df.index, dtype="Float64")
                        fx_eurpln = to_float64_nullable(df["Ex rate EUR"]) if "Ex rate EUR" in df.columns else pd.Series(pd.NA, index=df.index, dtype="Float64")
                        res = pd.Series(pd.NA, index=df.index, dtype="Float64")
                        res = res.mask(cur2.eq("EUR"), total)
                        res = res.mask(cur2.eq("USD"), total * fx_eurusd)
                        res = res.mask(cur2.eq("PLN"), total / fx_eurpln)
                        return res

                    out["Tonnes"] = out["Tonnes"].apply(clean_number)
                    if "Price FCA" in out.columns:
                        out["Price FCA"] = out["Price FCA"].apply(clean_number)
                        out["Total price FCA"] = out["Tonnes"] * out["Price FCA"]
                    if "Price Dap" in out.columns:
                        out["Price Dap"] = out["Price Dap"].apply(clean_number)
                        out["Total price DAP"] = out["Tonnes"] * out["Price Dap"]

                    out["Total Price FCA EUR"] = to_eur_any(out, "Total price FCA")
                    out["Total Price DAP EUR"] = to_eur_any(out, "Total price DAP")
                    out["Price FCA EUR"] = to_eur_any(out, "Price FCA")
                    out["Price DAP EUR"] = to_eur_any(out, "Price Dap")

            # Attach pickups to first row per contract (after ensuring existence)
            if "Contract" in out.columns and len(out) > 0:
                out = out.sort_values(["Contract", "Delivery month"], na_position="last").reset_index(drop=True)

                first_mask2 = ~out.duplicated(subset=["Contract"])
                base_first = out.loc[first_mask2].copy()
                base_rest = out.loc[~first_mask2].copy()

                base_first["Contract_key"] = base_first["Contract"].apply(norm_key)

                merged_first = base_first.merge(
                    wagi.drop(columns=["Contract"]),
                    on="Contract_key",
                    how="left",
                    suffixes=("", "_w"),
                )

                for col in ["Pick Up date", "Pick up quantity"]:
                    wcol = f"{col}_w"
                    if wcol in merged_first.columns:
                        merged_first[col] = merged_first[wcol]
                        merged_first = merged_first.drop(columns=[wcol])

                out = pd.concat(
                    [merged_first.drop(columns=["Contract_key"], errors="ignore"), base_rest],
                    ignore_index=True,
                    sort=False
                )

    # ----------------- FIX: duplicates across seasons -----------------
    # For duplicated contracts in SP: Goods sold should be SUM, Season should be Season1/Season2
    if "Contract" in out.columns and "Contract" in base_df.columns:
        b = base_df.copy()
        b["Contract_key"] = b["Contract"].apply(norm_key)

        contract_agg = {"Contract": "first"}
        if "Goods sold" in b.columns:
            contract_agg["Goods sold"] = sum_non_empty_number
        if "Season" in b.columns:
            contract_agg["Season"] = join_unique_non_empty_str

        contract_level = b.groupby("Contract_key", as_index=False).agg(contract_agg)

        out["Contract_key"] = out["Contract"].apply(norm_key)
        out = out.merge(
            contract_level[["Contract_key"] + [c for c in ["Goods sold", "Season"] if c in contract_level.columns]],
            on="Contract_key",
            how="left",
            suffixes=("", "_contract"),
        )

        if "Goods sold_contract" in out.columns:
            out["Goods sold"] = out["Goods sold_contract"]
            out = out.drop(columns=["Goods sold_contract"])

        if "Season_contract" in out.columns:
            out["Season"] = out["Season_contract"]
            out = out.drop(columns=["Season_contract"])

        out = out.drop(columns=["Contract_key"])

    # ---- Goods sold only once per contract (after all appends/merges) ----
    if "Contract" in out.columns:
        out = out.sort_values(["Contract", "Delivery month", "Pick Up date"], na_position="last").reset_index(drop=True)
        if "Goods sold" in out.columns:
            out["Goods sold"] = out["Goods sold"].apply(clean_number).fillna(0)
            first_mask = ~out.duplicated(subset=["Contract"])
            out.loc[~first_mask, "Goods sold"] = "-"

    # ---- Tonnes only once per Contract + Delivery month (so pivots work cleanly) ----
    if "Contract" in out.columns and "Delivery month" in out.columns:
        out = out.sort_values(["Contract", "Delivery month", "Pick Up date"], na_position="last").reset_index(drop=True)

        # ensure numeric, then blank out duplicates
        out["Tonnes"] = out["Tonnes"].apply(clean_number).fillna(0)

        first_tonnes_mask = ~out.duplicated(subset=["Contract", "Delivery month"])
        out.loc[~first_tonnes_mask, "Tonnes"] = "-"


    # ---- Pickup totals (AFTER wagi merge) ----
    def f64(colname: str) -> pd.Series:
        if colname not in out.columns:
            return pd.Series(pd.NA, index=out.index, dtype="Float64")
        return out[colname].map(clean_number).astype("Float64")

    pu_qty = f64("Pick up quantity")
    out["Pickup Total FCA"] = pu_qty * f64("Price FCA")
    out["Pickup Total DAP"] = pu_qty * f64("Price Dap")
    out["Pickup Total FCA EUR"] = pu_qty * f64("Price FCA EUR")
    out["Pickup Total DAP EUR"] = pu_qty * f64("Price DAP EUR")

    # Final column order
    out = out[[c for c in FINAL_ORDER if c in out.columns]]

    # ---- WRITE XLSX ----
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="Output")
        ws = writer.sheets["Output"]

        if "Date" in out.columns:
            date_col_idx = out.columns.get_loc("Date") + 1
            for r in range(2, ws.max_row + 1):
                cell = ws.cell(row=r, column=date_col_idx)
                if isinstance(cell.value, (datetime, pd.Timestamp)):
                    cell.number_format = "DD/MM/YYYY"

        if "Delivery month" in out.columns:
            dm_col_idx = out.columns.get_loc("Delivery month") + 1
            for r in range(2, ws.max_row + 1):
                cell = ws.cell(row=r, column=dm_col_idx)
                if isinstance(cell.value, (datetime, pd.Timestamp)):
                    cell.number_format = "mmm yy"

        if "Pick Up date" in out.columns:
            pu_col_idx = out.columns.get_loc("Pick Up date") + 1
            for r in range(2, ws.max_row + 1):
                cell = ws.cell(row=r, column=pu_col_idx)
                if isinstance(cell.value, (datetime, pd.Timestamp)):
                    cell.number_format = "DD.MM.YYYY"

    xlsx_bytes = buffer.getvalue()

    meta = {
        "header_row": header_row,
        "month_cols_detected": [str(k) for k in month_cols.keys()],
        "rows_out": int(out.shape[0]),
    }
    return xlsx_bytes, meta


# ---------------- STREAMLIT UI ----------------
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

        output_name = Path(uploaded.name).stem + "_output.xlsx"

        st.download_button(
            "Download Excel",
            data=xlsx_bytes,
            file_name=output_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        st.error(str(e))
