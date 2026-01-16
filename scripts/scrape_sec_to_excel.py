#!/usr/bin/env python3
"""
SEC v1 scraper (hard reset):
- Read tickers from CSV or XLSX (auto-detect).
- Fetch SEC ticker->CIK map, then company submissions -> SIC.
- Flag Aerospace & Defense via SIC allowlist.
- Write to data/defense_screening_prototype_v1.xlsx (Universe sheet).

Designed for manual, occasional runs (monthly).
"""

from __future__ import annotations

import csv
import json
import time
import zipfile
from dataclasses import dataclass
from datetime import date
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

# Output workbook (your "main" file)
OUTPUT_XLSX = DATA_DIR / "defense_screening_prototype_v1.xlsx"

# Universe input candidates (first existing is used)
UNIVERSE_CANDIDATES = [
    DATA_DIR / "universe.csv",
    DATA_DIR / "Universe.xlsx",
    DATA_DIR / "universe.xlsx",
    OUTPUT_XLSX,  # fallback: read tickers from the output workbook itself
]

UNIVERSE_SHEET = "Universe"
UNIVERSE_TICKER_HEADER = "ticker"  # case-insensitive header match

SIC_CONFIG = CONFIG_DIR / "sic_aero_defense.json"
DEFAULT_SIC_ALLOWLIST = [3480, 3720, 3721, 3724, 3728, 3760, 3812]

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL_TMPL = "https://data.sec.gov/submissions/CIK{cik10}.json"

# IMPORTANT: use a real UA with contact
USER_AGENT = "Ferdinand Niggemeier Small-Cap Stock Screener (Niggemeier.Ferdinand@gmail.com)"

SECONDS_BETWEEN_REQUESTS = 0.2


# -----------------------------
# Helpers: IO + validation
# -----------------------------

def sleep_rate_limit() -> None:
    time.sleep(SECONDS_BETWEEN_REQUESTS)

def ensure_sic_config() -> None:
    """
    Create SIC config if missing or empty.
    """
    if not SIC_CONFIG.exists() or SIC_CONFIG.stat().st_size < 5:
        SIC_CONFIG.write_text(
            json.dumps({"sic_allowlist": DEFAULT_SIC_ALLOWLIST}, indent=2),
            encoding="utf-8",
        )

def load_sic_allowlist() -> set[int]:
    ensure_sic_config()
    cfg = json.loads(SIC_CONFIG.read_text(encoding="utf-8"))
    return set(int(x) for x in cfg.get("sic_allowlist", []))

def is_valid_xlsx(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 100:
        return False
    try:
        with zipfile.ZipFile(path, "r") as z:
            z.namelist()
        return True
    except Exception:
        return False

def ensure_output_workbook() -> None:
    """
    Ensure OUTPUT_XLSX exists and is a valid .xlsx with a Universe sheet and headers.
    """
    if is_valid_xlsx(OUTPUT_XLSX):
        # also ensure Universe sheet exists
        wb = load_workbook(OUTPUT_XLSX)
        if UNIVERSE_SHEET not in wb.sheetnames:
            ws = wb.create_sheet(UNIVERSE_SHEET)
            ws.append([
                "Ticker","Company Name","CIK","SIC","SIC Description",
                "Aerospace & Defense","Data As-Of Date"
            ])
            wb.save(OUTPUT_XLSX)
        return

    # Create fresh workbook
    wb = Workbook()
    ws = wb.active
    ws.title = UNIVERSE_SHEET
    ws.append([
        "Ticker","Company Name","CIK","SIC","SIC Description",
        "Aerospace & Defense","Data As-Of Date"
    ])
    wb.save(OUTPUT_XLSX)

def resolve_universe_input() -> Path:
    """
    Pick the first existing universe input file. If none exist, error.
    """
    for p in UNIVERSE_CANDIDATES:
        if p.exists() and p.stat().st_size > 0:
            return p
    raise FileNotFoundError(
        "No universe input found. Create one of: "
        + ", ".join(str(p) for p in UNIVERSE_CANDIDATES[:-1])
        + " (or put tickers into the 'Universe' sheet of the output workbook)."
    )

def normalize_header(s: str) -> str:
    return " ".join(s.strip().lower().split())

def load_tickers_from_csv(path: Path) -> List[str]:
    tickers: List[str] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames and any(fn and fn.lower() == "ticker" for fn in reader.fieldnames):
            for row in reader:
                t = (row.get("ticker") or "").strip().upper()
                if t:
                    tickers.append(t)
        else:
            f.seek(0)
            raw = csv.reader(f)
            for r in raw:
                if not r:
                    continue
                t = (r[0] or "").strip().upper()
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

    # Find header row in first 20 rows
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
        t = str(v).strip().upper()
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

def dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# -----------------------------
# SEC fetch helpers (with cache)
# -----------------------------

def get_json(url: str, cache_key: str) -> Dict[str, Any]:
    cache_path = CACHE_DIR / f"{cache_key}.json"
    if cache_path.exists() and cache_path.stat().st_size > 10:
        return json.loads(cache_path.read_text(encoding="utf-8"))

    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    sleep_rate_limit()
    resp.raise_for_status()
    data = resp.json()
    cache_path.write_text(json.dumps(data), encoding="utf-8")
    return data

def build_ticker_to_cik_map() -> Dict[str, Tuple[str, str]]:
    data = get_json(SEC_COMPANY_TICKERS_URL, cache_key="company_tickers")
    mapping: Dict[str, Tuple[str, str]] = {}
    for _, rec in data.items():
        ticker = str(rec.get("ticker", "")).upper().strip()
        cik_str = str(rec.get("cik_str", "")).strip()
        title = str(rec.get("title", "")).strip()
        if ticker and cik_str.isdigit():
            mapping[ticker] = (cik_str.zfill(10), title)
    return mapping

def fetch_company_submissions(cik10: str) -> Dict[str, Any]:
    url = SEC_SUBMISSIONS_URL_TMPL.format(cik10=cik10)
    return get_json(url, cache_key=f"submissions_{cik10}")

def extract_sic(submissions: Dict[str, Any]) -> Tuple[Optional[int], Optional[str]]:
    sic_raw = submissions.get("sic")
    sic_desc = submissions.get("sicDescription")
    sic = None
    if sic_raw is not None:
        try:
            sic = int(str(sic_raw).strip())
        except ValueError:
            sic = None
    if sic_desc is not None:
        sic_desc = str(sic_desc).strip()
    return sic, sic_desc


# -----------------------------
# Excel writing
# -----------------------------

@dataclass
class CompanyRow:
    ticker: str
    cik: str
    name: str
    sic: Optional[int]
    sic_desc: Optional[str]
    is_aero_defense: bool
    data_as_of: str

def find_header_row(ws: Worksheet, max_rows: int = 20) -> Tuple[int, Dict[str, int]]:
    best_row = None
    best_map: Dict[str, int] = {}

    for r in range(1, max_rows + 1):
        row_map: Dict[str, int] = {}
        for c in range(1, ws.max_column + 1):
            v = ws.cell(row=r, column=c).value
            if isinstance(v, str) and v.strip():
                row_map[normalize_header(v)] = c
        known = sum(1 for k in row_map.keys() if k in {
            "ticker", "symbol", "cik", "company name", "name", "sic", "sic description"
        })
        if known >= 2 and len(row_map) > len(best_map):
            best_row = r
            best_map = row_map

    if best_row is None:
        best_row = 1
        best_map = {}
        for c in range(1, ws.max_column + 1):
            v = ws.cell(row=1, column=c).value
            if isinstance(v, str) and v.strip():
                best_map[normalize_header(v)] = c

    return best_row, best_map

def col_for(header_map: Dict[str, int], *candidates: str) -> Optional[int]:
    for cand in candidates:
        c = header_map.get(normalize_header(cand))
        if c:
            return c
    return None

def write_rows_to_excel(xlsx_path: Path, rows: List[CompanyRow]) -> None:
    wb = load_workbook(xlsx_path)
    if UNIVERSE_SHEET not in wb.sheetnames:
        ws = wb.create_sheet(UNIVERSE_SHEET)
        ws.append([
            "Ticker","Company Name","CIK","SIC","SIC Description",
            "Aerospace & Defense","Data As-Of Date"
        ])
        wb.save(xlsx_path)
        wb = load_workbook(xlsx_path)

    ws = wb[UNIVERSE_SHEET]
    header_row, header_map = find_header_row(ws)

    c_ticker = col_for(header_map, "Ticker", "Symbol")
    c_name = col_for(header_map, "Company Name", "Name", "Company")
    c_cik = col_for(header_map, "CIK")
    c_sic = col_for(header_map, "SIC")
    c_sic_desc = col_for(header_map, "SIC Description", "SIC Desc", "SIC Industry")
    c_flag = col_for(header_map, "Aerospace & Defense", "Aerospace and Defense", "A&D", "Aerospace & Defense?")
    c_asof = col_for(header_map, "Data As-Of Date", "Data As Of", "As Of")

    # Find first empty row by checking the ticker column if present; else column 1
    check_col = c_ticker or 1
    start_row = header_row + 1
    while ws.cell(row=start_row, column=check_col).value not in (None, ""):
        start_row += 1

    for i, r in enumerate(rows):
        rr = start_row + i
        if c_ticker: ws.cell(row=rr, column=c_ticker).value = r.ticker
        if c_name: ws.cell(row=rr, column=c_name).value = r.name
        if c_cik: ws.cell(row=rr, column=c_cik).value = r.cik
        if c_sic: ws.cell(row=rr, column=c_sic).value = r.sic
        if c_sic_desc: ws.cell(row=rr, column=c_sic_desc).value = r.sic_desc
        if c_flag: ws.cell(row=rr, column=c_flag).value = "TRUE" if r.is_aero_defense else "FALSE"
        if c_asof: ws.cell(row=rr, column=c_asof).value = r.data_as_of

    wb.save(xlsx_path)


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    ensure_output_workbook()
    sic_allow = load_sic_allowlist()

    universe_path = resolve_universe_input()
    tickers = load_tickers_any(universe_path)

    if not tickers:
        raise RuntimeError(f"No tickers found in universe input: {universe_path}")

    t2c = build_ticker_to_cik_map()
    as_of = date.today().isoformat()

    out_rows: List[CompanyRow] = []
    for t in tickers:
        if t not in t2c:
            continue
        cik10, name = t2c[t]
        subs = fetch_company_submissions(cik10)
        sic, sic_desc = extract_sic(subs)
        is_ad = (sic in sic_allow) if sic is not None else False
        out_rows.append(
            CompanyRow(
                ticker=t,
                cik=cik10,
                name=name,
                sic=sic,
                sic_desc=sic_desc,
                is_aero_defense=is_ad,
                data_as_of=as_of,
            )
        )

    write_rows_to_excel(OUTPUT_XLSX, out_rows)
    print(f"Universe input: {universe_path}")
    print(f"Wrote {len(out_rows)} rows to {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
