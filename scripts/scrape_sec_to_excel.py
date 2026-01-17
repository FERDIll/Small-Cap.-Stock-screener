#!/usr/bin/env python3
"""
SEC small-cap screener scraper — Gate 1 only (Reporting / Investable)

Gate 1 (binary):
PASS if any of:
- Latest 10-K filed within last 18 months
- Latest 20-F filed within last 24 months (foreign issuer equivalent)
- Recent IPO proxy: S-1 filed within last 240 days (grace window)

Design goals:
- Never crash: missing data -> "N/A"
- Update existing rows by ticker (do not only append)
- Read tickers from CSV or XLSX (auto-detect), fallback to output workbook
- Cache SEC responses on disk
"""

from __future__ import annotations

import csv
import json
import time
import zipfile
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.worksheet import Worksheet


# -----------------------------
# Configuration
# -----------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = REPO_ROOT / "data"
CONFIG_DIR = REPO_ROOT / "config"
CACHE_DIR = REPO_ROOT / ".cache" / "sec"

DATA_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_XLSX = DATA_DIR / "defense_screening_prototype_v1.xlsx"

UNIVERSE_CANDIDATES = [
    DATA_DIR / "universe.csv",
    DATA_DIR / "Universe.xlsx",
    DATA_DIR / "universe.xlsx",
    OUTPUT_XLSX,  # fallback: read tickers from output workbook itself
]

UNIVERSE_SHEET = "Universe"
UNIVERSE_TICKER_HEADER = "ticker"  # case-insensitive

SIC_CONFIG = CONFIG_DIR / "sic_aero_defense.json"
DEFAULT_SIC_ALLOWLIST = [3480, 3720, 3721, 3724, 3728, 3760, 3812]

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL_TMPL = "https://data.sec.gov/submissions/CIK{cik10}.json"

# IMPORTANT: SEC requests a real UA with contact info
USER_AGENT = "Ferdinand Niggemeier Small-Cap Stock Screener (Niggemeier.Ferdinand@gmail.com)"

SECONDS_BETWEEN_REQUESTS = 0.2
HTTP_TIMEOUT = 30

# Gate windows
G1_10K_MAX_AGE_DAYS = 18 * 30  # ~18 months
G1_20F_MAX_AGE_DAYS = 24 * 30  # ~24 months
G1_IPO_S1_MAX_AGE_DAYS = 240   # ~8 months


# -----------------------------
# Helpers: basic utils
# -----------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,9}$")  # allows BRK.B, RDS-A, etc.

def clean_ticker(raw: str) -> Optional[str]:
    t = (raw or "").strip().upper()
    if not t:
        return None
    if not TICKER_RE.match(t):
        return None
    return t

def sleep_rate_limit() -> None:
    time.sleep(SECONDS_BETWEEN_REQUESTS)

def parse_date(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def na(v: Any) -> Any:
    """Normalize missing values to 'N/A' (Excel-friendly)."""
    if v is None:
        return "N/A"
    if isinstance(v, str) and not v.strip():
        return "N/A"
    return v

def normalize_header(s: str) -> str:
    return " ".join(str(s).strip().lower().split())

def dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# -----------------------------
# Config: SIC allowlist
# -----------------------------

def ensure_sic_config() -> None:
    if not SIC_CONFIG.exists() or SIC_CONFIG.stat().st_size < 5:
        SIC_CONFIG.write_text(
            json.dumps({"sic_allowlist": DEFAULT_SIC_ALLOWLIST}, indent=2),
            encoding="utf-8",
        )

def load_sic_allowlist() -> set[int]:
    ensure_sic_config()
    cfg = json.loads(SIC_CONFIG.read_text(encoding="utf-8"))
    return set(int(x) for x in cfg.get("sic_allowlist", []))


# -----------------------------
# Universe input
# -----------------------------

def is_valid_xlsx(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 100:
        return False
    try:
        with zipfile.ZipFile(path, "r") as z:
            z.namelist()
        return True
    except Exception:
        return False

def resolve_universe_input() -> Path:
    for p in UNIVERSE_CANDIDATES:
        if p.exists() and p.stat().st_size > 0:
            return p
    raise FileNotFoundError(
        "No universe input found. Create one of: "
        + ", ".join(str(p) for p in UNIVERSE_CANDIDATES[:-1])
        + " (or put tickers into the 'Universe' sheet of the output workbook)."
    )

def load_tickers_from_csv(path: Path) -> List[str]:
    tickers: List[str] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames and any(fn and fn.strip().lower() == "ticker" for fn in reader.fieldnames):
            for row in reader:
                t = clean_ticker(row.get("ticker") or "")
                if t:
                    tickers.append(t)
        else:
            f.seek(0)
            raw = csv.reader(f)
            for r in raw:
                if not r:
                    continue
                t = clean_ticker(r[0] if r else "")
                if t and t != "TICKER":
                    tickers.append(t)
    return dedupe_preserve_order(tickers)

def load_tickers_from_xlsx(path: Path, sheet_name: str, ticker_header: str) -> List[str]:
    if not is_valid_xlsx(path):
        raise RuntimeError(f"{path} is not a valid .xlsx file.")
    wb = load_workbook(path, read_only=True, data_only=True)
    if sheet_name not in wb.sheetnames:
        raise RuntimeError(f"Sheet '{sheet_name}' not found in {path}")
    ws = wb[sheet_name]

    header_row = None
    header_map: Dict[str, int] = {}

    for r in range(1, min(20, ws.max_row) + 1):
        row_map: Dict[str, int] = {}
        for c in range(1, ws.max_column + 1):
            v = ws.cell(row=r, column=c).value
            if isinstance(v, str) and v.strip():
                row_map[normalize_header(v)] = c
        if normalize_header(ticker_header) in row_map:
            header_row = r
            header_map = row_map
            break

    if header_row is None:
        raise RuntimeError(f"Could not find a '{ticker_header}' column in {path} / sheet '{sheet_name}'")

    tcol = header_map[normalize_header(ticker_header)]
    tickers: List[str] = []
    for r in range(header_row + 1, ws.max_row + 1):
        v = ws.cell(row=r, column=tcol).value
        if v is None:
            continue
        t = clean_ticker(str(v))
        if t:
            tickers.append(t)

    return dedupe_preserve_order(tickers)

def load_tickers_any(path: Path) -> List[str]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return load_tickers_from_csv(path)
    if suffix in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        return load_tickers_from_xlsx(path, UNIVERSE_SHEET, UNIVERSE_TICKER_HEADER)
    raise ValueError(f"Unsupported universe input type: {path}")


# -----------------------------
# SEC fetch helpers (cached, safe)
# -----------------------------

def _http_headers() -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
    }

def get_json_cached(url: str, cache_key: str) -> Optional[Dict[str, Any]]:
    """
    Fetch JSON with a simple on-disk cache.
    Returns None on failure (never raises).
    """
    cache_path = CACHE_DIR / f"{cache_key}.json"
    if cache_path.exists() and cache_path.stat().st_size > 10:
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    try:
        resp = requests.get(url, headers=_http_headers(), timeout=HTTP_TIMEOUT)
        sleep_rate_limit()
        resp.raise_for_status()
        data = resp.json()
        cache_path.write_text(json.dumps(data), encoding="utf-8")
        return data
    except Exception:
        return None

def build_ticker_to_cik_map() -> Dict[str, Tuple[str, str]]:
    data = get_json_cached(SEC_COMPANY_TICKERS_URL, cache_key="company_tickers")
    mapping: Dict[str, Tuple[str, str]] = {}
    if not data:
        return mapping
    for _, rec in data.items():
        ticker = str(rec.get("ticker", "")).upper().strip()
        cik_str = str(rec.get("cik_str", "")).strip()
        title = str(rec.get("title", "")).strip()
        if ticker and cik_str.isdigit():
            mapping[ticker] = (cik_str.zfill(10), title)
    return mapping

def fetch_company_submissions(cik10: str) -> Optional[Dict[str, Any]]:
    url = SEC_SUBMISSIONS_URL_TMPL.format(cik10=cik10)
    return get_json_cached(url, cache_key=f"submissions_{cik10}")

def extract_sic(submissions: Dict[str, Any]) -> Tuple[Optional[int], Optional[str]]:
    sic_raw = (submissions or {}).get("sic")
    sic_desc = (submissions or {}).get("sicDescription")
    sic = None
    if sic_raw is not None:
        try:
            sic = int(str(sic_raw).strip())
        except Exception:
            sic = None
    if sic_desc is not None:
        sic_desc = str(sic_desc).strip()
    return sic, sic_desc


# -----------------------------
# Gate 1: reporting / investable check
# -----------------------------

def _latest_filing_date(submissions: Dict[str, Any], form_set: set[str]) -> Optional[date]:
    recent = (((submissions or {}).get("filings") or {}).get("recent") or {})
    forms = recent.get("form") or []
    dates = recent.get("filingDate") or []

    best: Optional[date] = None
    for i, f in enumerate(forms):
        if f not in form_set:
            continue
        if i >= len(dates):
            continue
        dd = parse_date(str(dates[i]))
        if dd and (best is None or dd > best):
            best = dd
    return best

def _count_forms_in_window(submissions: Dict[str, Any], form_set: set[str], days: int) -> int:
    recent = (((submissions or {}).get("filings") or {}).get("recent") or {})
    forms = recent.get("form") or []
    dates = recent.get("filingDate") or []

    cutoff = date.today() - timedelta(days=days)
    c = 0
    for i, f in enumerate(forms):
        if f not in form_set:
            continue
        if i >= len(dates):
            continue
        dd = parse_date(str(dates[i]))
        if dd and dd >= cutoff:
            c += 1
    return c

def gate1_reporting(submissions: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns Gate 1 decision + supporting fields.
    """
    today = date.today()

    latest_10k = _latest_filing_date(submissions, {"10-K", "10-K/A"})
    latest_10q = _latest_filing_date(submissions, {"10-Q", "10-Q/A"})
    latest_8k  = _latest_filing_date(submissions, {"8-K", "8-K/A"})
    latest_20f = _latest_filing_date(submissions, {"20-F", "20-F/A"})
    latest_6k  = _latest_filing_date(submissions, {"6-K"})
    latest_s1  = _latest_filing_date(submissions, {"S-1", "S-1/A"})

    pass_10k = bool(latest_10k and (today - latest_10k).days <= G1_10K_MAX_AGE_DAYS)
    pass_20f = bool(latest_20f and (today - latest_20f).days <= G1_20F_MAX_AGE_DAYS)
    pass_ipo = bool(latest_s1 and (today - latest_s1).days <= G1_IPO_S1_MAX_AGE_DAYS)

    passed = pass_10k or pass_20f or pass_ipo

    if passed:
        if pass_10k:
            reason = "PASS: 10-K within ~18 months"
        elif pass_20f:
            reason = "PASS: 20-F within ~24 months"
        else:
            reason = "PASS: Recent IPO proxy (S-1 within ~240 days)"
    else:
        # More informative fail reason:
        if latest_10k or latest_20f:
            reason = "FAIL: Annual report exists but is stale (too old)"
        elif latest_s1:
            reason = "FAIL: S-1 exists but outside IPO grace; no recent annual report"
        else:
            reason = "FAIL: No 10-K/20-F found (likely non-reporting / inactive)"

    # counts are useful for debugging / screening
    c10k_365 = _count_forms_in_window(submissions, {"10-K", "10-K/A"}, 365)
    c10q_365 = _count_forms_in_window(submissions, {"10-Q", "10-Q/A"}, 365)
    c8k_365  = _count_forms_in_window(submissions, {"8-K", "8-K/A"}, 365)

    recent = (((submissions or {}).get("filings") or {}).get("recent") or {})
    n_recent = len(recent.get("form") or [])

    return {
        "G1_Pass_Reporting": "TRUE" if passed else "FALSE",
        "G1_Reason": reason,

        "C_10K_Count_365d": c10k_365,
        "C_10Q_Count_365d": c10q_365,
        "C_8K_Count_365d": c8k_365,

        "C_Latest_10K_Date": latest_10k.isoformat() if latest_10k else None,
        "C_Latest_10Q_Date": latest_10q.isoformat() if latest_10q else None,
        "C_Latest_8K_Date": latest_8k.isoformat() if latest_8k else None,

        "C_Latest_20F_Date": latest_20f.isoformat() if latest_20f else None,
        "C_Latest_6K_Date": latest_6k.isoformat() if latest_6k else None,
        "C_Latest_S1_Date": latest_s1.isoformat() if latest_s1 else None,

        "C_Recent_Filings_Listed": n_recent,
    }


# -----------------------------
# Excel writing (robust, update-by-ticker)
# -----------------------------

BASE_HEADERS = [
    "Ticker",
    "Company Name",
    "CIK",
    "SIC",
    "SIC Description",
    "Aerospace & Defense",
    "Data As-Of Date",
    "Run Timestamp (UTC)",
    "Status",
    "Status Detail",
]

def ensure_output_workbook() -> None:
    if is_valid_xlsx(OUTPUT_XLSX):
        wb = load_workbook(OUTPUT_XLSX)
        if UNIVERSE_SHEET not in wb.sheetnames:
            ws = wb.create_sheet(UNIVERSE_SHEET)
            ws.append(BASE_HEADERS)
            wb.save(OUTPUT_XLSX)
        return

    wb = Workbook()
    ws = wb.active
    ws.title = UNIVERSE_SHEET
    ws.append(BASE_HEADERS)
    wb.save(OUTPUT_XLSX)

def find_header_row(ws: Worksheet, max_rows: int = 20) -> Tuple[int, Dict[str, int]]:
    for r in range(1, max_rows + 1):
        row_map: Dict[str, int] = {}
        for c in range(1, ws.max_column + 1):
            v = ws.cell(row=r, column=c).value
            if isinstance(v, str) and v.strip():
                row_map[normalize_header(v)] = c
        if "ticker" in row_map and ("cik" in row_map or "company name" in row_map):
            return r, row_map

    # fallback to row 1
    row_map = {}
    for c in range(1, ws.max_column + 1):
        v = ws.cell(row=1, column=c).value
        if isinstance(v, str) and v.strip():
            row_map[normalize_header(v)] = c
    return 1, row_map

def ensure_columns(ws: Worksheet, header_row: int, header_map: Dict[str, int], columns: List[str]) -> Dict[str, int]:
    max_col = ws.max_column
    for col_name in columns:
        key = normalize_header(col_name)
        if key in header_map:
            continue
        max_col += 1
        ws.cell(row=header_row, column=max_col).value = col_name
        header_map[key] = max_col
    return header_map

def build_ticker_row_index(ws: Worksheet, header_row: int, ticker_col: int) -> Dict[str, int]:
    idx: Dict[str, int] = {}
    for r in range(header_row + 1, ws.max_row + 1):
        v = ws.cell(row=r, column=ticker_col).value
        if not v:
            continue
        t = str(v).strip().upper()
        if t:
            idx[t] = r
    return idx

def write_company_dicts_to_excel(xlsx_path: Path, rows: List[Dict[str, Any]]) -> None:
    wb = load_workbook(xlsx_path)
    if UNIVERSE_SHEET not in wb.sheetnames:
        ws = wb.create_sheet(UNIVERSE_SHEET)
        ws.append(BASE_HEADERS)
    ws = wb[UNIVERSE_SHEET]

    header_row, header_map = find_header_row(ws)

    dynamic_cols: List[str] = []
    for r in rows:
        for k in r.keys():
            if k not in dynamic_cols and k not in BASE_HEADERS:
                dynamic_cols.append(k)

    all_cols = BASE_HEADERS + dynamic_cols
    header_map = ensure_columns(ws, header_row, header_map, all_cols)

    ticker_col = header_map.get("ticker")
    if not ticker_col:
        header_map = ensure_columns(ws, header_row, header_map, ["Ticker"])
        ticker_col = header_map["ticker"]

    existing = build_ticker_row_index(ws, header_row, ticker_col)

    append_row = ws.max_row + 1
    while ws.cell(row=append_row, column=ticker_col).value not in (None, ""):
        append_row += 1

    for rd in rows:
        ticker = str(rd.get("Ticker") or rd.get("ticker") or "").strip().upper()
        if not ticker:
            continue

        row_num = existing.get(ticker)
        if row_num is None:
            row_num = append_row
            append_row += 1
            existing[ticker] = row_num

        for k, v in rd.items():
            col = header_map.get(normalize_header(k))
            if not col:
                header_map = ensure_columns(ws, header_row, header_map, [k])
                col = header_map[normalize_header(k)]
            ws.cell(row=row_num, column=col).value = na(v)

    wb.save(xlsx_path)


# -----------------------------
# Main
# -----------------------------

def build_base_row(
    ticker: str,
    cik10: Optional[str],
    name: Optional[str],
    sic: Optional[int],
    sic_desc: Optional[str],
    is_ad: Optional[bool],
    *,
    status: str,
    status_detail: Optional[str],
    as_of: str,
) -> Dict[str, Any]:
    return {
        "Ticker": ticker,
        "Company Name": name,
        "CIK": cik10,
        "SIC": sic,
        "SIC Description": sic_desc,
        "Aerospace & Defense": "TRUE" if is_ad else ("FALSE" if is_ad is not None else None),
        "Data As-Of Date": as_of,
        "Run Timestamp (UTC)": now_iso(),
        "Status": status,
        "Status Detail": status_detail,
    }

def main() -> None:
    ensure_output_workbook()
    sic_allow = load_sic_allowlist()

    universe_path = resolve_universe_input()
    tickers = load_tickers_any(universe_path)
    if not tickers:
        raise RuntimeError(f"No tickers found in universe input: {universe_path}")

    t2c = build_ticker_to_cik_map()
    as_of = date.today().isoformat()

    out_rows: List[Dict[str, Any]] = []

    for t in tickers:
        t = (t or "").strip().upper()
        if not t:
            continue

        if t not in t2c:
            out_rows.append(
                build_base_row(
                    ticker=t,
                    cik10=None,
                    name=None,
                    sic=None,
                    sic_desc=None,
                    is_ad=None,
                    status="Excluded",
                    status_detail="Ticker not found in SEC ticker map",
                    as_of=as_of,
                )
            )
            continue

        cik10, name = t2c[t]

        try:
            submissions = fetch_company_submissions(cik10)
            if not submissions:
                out_rows.append(
                    build_base_row(
                        ticker=t,
                        cik10=cik10,
                        name=name,
                        sic=None,
                        sic_desc=None,
                        is_ad=None,
                        status="Excluded",
                        status_detail="No submissions JSON (SEC data unavailable)",
                        as_of=as_of,
                    )
                )
                continue

            sic, sic_desc = extract_sic(submissions)
            is_ad = (sic in sic_allow) if sic is not None else False

            # Gate 1 decision + Tier-C style filing stats (only from submissions)
            g1 = gate1_reporting(submissions)

            status = "OK" if g1.get("G1_Pass_Reporting") == "TRUE" else "Excluded"
            out = build_base_row(
                ticker=t,
                cik10=cik10,
                name=name,
                sic=sic,
                sic_desc=sic_desc,
                is_ad=is_ad,
                status=status,
                status_detail=g1.get("G1_Reason"),
                as_of=as_of,
            )
            out.update(g1)
            out_rows.append(out)

        except Exception:
            out_rows.append(
                build_base_row(
                    ticker=t,
                    cik10=cik10,
                    name=name,
                    sic=None,
                    sic_desc=None,
                    is_ad=None,
                    status="Excluded",
                    status_detail="Unexpected error in company loop",
                    as_of=as_of,
                )
            )

    write_company_dicts_to_excel(OUTPUT_XLSX, out_rows)

    print(f"Universe input: {universe_path}")
    print(f"Processed tickers: {len(tickers)}")
    print(f"Updated workbook: {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
