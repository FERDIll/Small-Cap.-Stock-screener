#!/usr/bin/env python3
"""
SEC small-cap screener scraper (tiered, failure-tolerant)

Tiers:
- Tier A: Structured XBRL/DEI facts (reliable) via SEC companyfacts JSON
- Tier B: Insider behavior (Form 4) via XML parsing (optional; best-effort)
- Tier C: Filing-type signals from submissions JSON (reliable enough)
- Tier D: Text-mined disclosures (NOT implemented here; keep out of core)

Design goals:
- Never crash the run because one company is missing data.
- Any missing/failed datapoint becomes "N/A" in the output workbook.
- Update existing rows by ticker instead of only appending.
"""

from __future__ import annotations

import csv
import json
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree as ET

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
    OUTPUT_XLSX,  # fallback
]

UNIVERSE_SHEET = "Universe"
UNIVERSE_TICKER_HEADER = "ticker"  # case-insensitive

SIC_CONFIG = CONFIG_DIR / "sic_aero_defense.json"
DEFAULT_SIC_ALLOWLIST = [3480, 3720, 3721, 3724, 3728, 3760, 3812]

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL_TMPL = "https://data.sec.gov/submissions/CIK{cik10}.json"
SEC_COMPANYFACTS_URL_TMPL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"

SEC_ARCHIVES_PRIMARYDOC_TMPL = (
    "https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{acc_nodash}/{primary_doc}"
)

# IMPORTANT: SEC requests a real UA with contact info
USER_AGENT = "Ferdinand Niggemeier Small-Cap Stock Screener (Niggemeier.Ferdinand@gmail.com)"

SECONDS_BETWEEN_REQUESTS = 0.2
HTTP_TIMEOUT = 30

# Switches (you can turn Tier B off if you want speed/reliability)
ENABLE_TIER_A = True
ENABLE_TIER_B = True   # Form 4 parsing (best-effort)
ENABLE_TIER_C = True


# -----------------------------
# Helpers: basic utils
# -----------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

import re

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

def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None

def normalize_header(s: str) -> str:
    return " ".join(str(s).strip().lower().split())


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

def dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

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

def get_text(url: str, cache_key: str) -> Optional[str]:
    """
    Fetch text (e.g., Form 4 XML) with cache.
    Returns None on failure.
    """
    cache_path = CACHE_DIR / f"{cache_key}.txt"
    if cache_path.exists() and cache_path.stat().st_size > 10:
        try:
            return cache_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            pass

    try:
        resp = requests.get(url, headers=_http_headers(), timeout=HTTP_TIMEOUT)
        sleep_rate_limit()
        resp.raise_for_status()
        text = resp.text
        cache_path.write_text(text, encoding="utf-8")
        return text
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

def fetch_company_facts(cik10: str) -> Optional[Dict[str, Any]]:
    url = SEC_COMPANYFACTS_URL_TMPL.format(cik10=cik10)
    return get_json_cached(url, cache_key=f"companyfacts_{cik10}")

def extract_sic(submissions: Dict[str, Any]) -> Tuple[Optional[int], Optional[str]]:
    sic_raw = submissions.get("sic")
    sic_desc = submissions.get("sicDescription")
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
# Tier A: XBRL/DEI metrics (best effort)
# -----------------------------

GAAP_TAGS = {
    # Revenue
    "revenue": ["Revenues", "SalesRevenueNet"],
    # Operating income
    "op_income": ["OperatingIncomeLoss"],
    # R&D
    "rnd": ["ResearchAndDevelopmentExpense"],
    # SG&A
    "sga": ["SellingGeneralAndAdministrativeExpense"],
    # CFO
    "ocf": ["NetCashProvidedByUsedInOperatingActivities"],
    # Capex
    "capex": ["PaymentsToAcquirePropertyPlantAndEquipment"],
    # Cash
    "cash": ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"],
    "cash_st_inv": ["CashAndCashEquivalentsAndShortTermInvestments"],
    # Debt
    "debt_lt": ["LongTermDebtNoncurrent", "LongTermDebt"],
    "debt_st": ["DebtCurrent", "LongTermDebtCurrent"],
    # Balance sheet
    "assets": ["Assets"],
    "current_assets": ["AssetsCurrent"],
    "current_liab": ["LiabilitiesCurrent"],
    "equity": ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"],
    # SBC
    "sbc": ["ShareBasedCompensation"],
}

DEI_TAGS = {
    "shares_out": ["EntityCommonStockSharesOutstanding"],
}

def _facts_units(facts: Dict[str, Any], namespace: str, tag: str) -> Optional[Dict[str, Any]]:
    try:
        return facts["facts"][namespace][tag]["units"]
    except Exception:
        return None

def _iter_fact_points(units: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    # units keys might be "USD", "shares", etc. pick all and iterate
    for _, arr in units.items():
        if isinstance(arr, list):
            for p in arr:
                if isinstance(p, dict):
                    yield p

def _pick_period_points(
    facts: Dict[str, Any],
    namespace: str,
    tags: List[str],
    *,
    forms: Optional[set[str]] = None,
    fps: Optional[set[str]] = None,
) -> List[Dict[str, Any]]:
    points: List[Dict[str, Any]] = []
    for tag in tags:
        units = _facts_units(facts, namespace, tag)
        if not units:
            continue
        for p in _iter_fact_points(units):
            if forms and p.get("form") not in forms:
                continue
            if fps and p.get("fp") not in fps:
                continue
            if not p.get("end") or p.get("val") is None:
                continue
            points.append(p)
    # sort by end date
    points.sort(key=lambda x: x.get("end", ""))
    return points

def _latest_instant(points: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    # For instant facts like shares outstanding, balance sheet: take latest by end
    if not points:
        return None
    return points[-1]

def _latest_four_quarters(points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Prefer quarterly points (fp Q1/Q2/Q3/Q4 if present; SEC uses Q1/Q2/Q3 and FY)
    # We'll just take last 4 by end date for points already filtered to quarterly-style
    if len(points) <= 4:
        return points
    return points[-4:]

def _sum_vals(points: List[Dict[str, Any]]) -> Optional[float]:
    vals = [safe_float(p.get("val")) for p in points]
    vals = [v for v in vals if v is not None]
    if not vals or len(vals) != len(points):
        return None
    return float(sum(vals))

def _yoy_growth(latest: float, prev: float) -> Optional[float]:
    if latest is None or prev is None:
        return None
    if prev == 0:
        return None
    return (latest / prev) - 1.0

def tier_a_metrics(companyfacts: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns a dict of Tier A metrics. Any missing becomes 'N/A' later.
    """
    out: Dict[str, Any] = {}

    forms = {"10-Q", "10-K"}
    # Income statement flow tags: prefer quarterly points where possible
    # We include fp in {"Q1","Q2","Q3","FY"} and then heuristically compute.
    fps_flow = {"Q1", "Q2", "Q3", "FY"}

    # Revenue points
    rev_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["revenue"], forms=forms, fps=fps_flow)
    opinc_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["op_income"], forms=forms, fps=fps_flow)
    rnd_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["rnd"], forms=forms, fps=fps_flow)
    sga_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["sga"], forms=forms, fps=fps_flow)
    ocf_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["ocf"], forms=forms, fps=fps_flow)
    capex_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["capex"], forms=forms, fps=fps_flow)
    sbc_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["sbc"], forms=forms, fps=fps_flow)

    # Balance sheet instants
    fps_inst = {"Q1", "Q2", "Q3", "FY"}
    cash_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["cash_st_inv"], forms=forms, fps=fps_inst)
    if not cash_points:
        cash_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["cash"], forms=forms, fps=fps_inst)

    debt_lt_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["debt_lt"], forms=forms, fps=fps_inst)
    debt_st_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["debt_st"], forms=forms, fps=fps_inst)

    curr_assets_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["current_assets"], forms=forms, fps=fps_inst)
    curr_liab_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["current_liab"], forms=forms, fps=fps_inst)
    equity_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["equity"], forms=forms, fps=fps_inst)

    # Shares outstanding (DEI)
    shares_points = _pick_period_points(companyfacts, "dei", DEI_TAGS["shares_out"], forms=forms, fps=fps_inst)

    # --- Compute TTM revenue (prefer last 4 quarters if we can approximate) ---
    # Best-effort: If we have >=4 points, take last 4 values and sum.
    # If not, fall back to latest FY revenue.
    ttm_rev = None
    ttm_rev_end = None

    if len(rev_points) >= 4:
        last4 = _latest_four_quarters(rev_points)
        ttm_rev = _sum_vals(last4)
        ttm_rev_end = last4[-1].get("end")
    else:
        # fallback to latest FY
        fy = [p for p in rev_points if p.get("fp") == "FY"]
        if fy:
            last = fy[-1]
            ttm_rev = safe_float(last.get("val"))
            ttm_rev_end = last.get("end")

    out["A_TTM_Revenue_USD"] = ttm_rev
    out["A_TTM_Revenue_End"] = ttm_rev_end

    # YoY growth (quarterly-style if possible; else FY YoY)
    yoy = None
    if len(rev_points) >= 8:
        latest4 = _latest_four_quarters(rev_points)     # not used directly
        latest = rev_points[-1]
        prev_year = rev_points[-5]  # roughly same quarter prior year if series is quarterly-ish
        yoy = _yoy_growth(safe_float(latest.get("val")), safe_float(prev_year.get("val")))
    else:
        fy = [p for p in rev_points if p.get("fp") == "FY"]
        if len(fy) >= 2:
            yoy = _yoy_growth(safe_float(fy[-1].get("val")), safe_float(fy[-2].get("val")))

    out["A_Revenue_YoY_Growth"] = yoy

    # Operating margin (best-effort: latest period op income / latest period revenue)
    op_margin = None
    if rev_points and opinc_points:
        rev_last = safe_float(rev_points[-1].get("val"))
        op_last = safe_float(opinc_points[-1].get("val"))
        if rev_last and rev_last != 0 and op_last is not None:
            op_margin = op_last / rev_last
    out["A_Operating_Margin"] = op_margin

    # R&D intensity and SG&A intensity (latest period)
    rnd_int = None
    if rev_points and rnd_points:
        rev_last = safe_float(rev_points[-1].get("val"))
        rnd_last = safe_float(rnd_points[-1].get("val"))
        if rev_last and rev_last != 0 and rnd_last is not None:
            rnd_int = rnd_last / rev_last
    out["A_RnD_Intensity"] = rnd_int

    sga_int = None
    if rev_points and sga_points:
        rev_last = safe_float(rev_points[-1].get("val"))
        sga_last = safe_float(sga_points[-1].get("val"))
        if rev_last and rev_last != 0 and sga_last is not None:
            sga_int = sga_last / rev_last
    out["A_SGandA_Intensity"] = sga_int

    # Cash burn / runway: use latest cash and last-4-quarter OCF/FCF
    cash_latest = None
    cash_end = None
    li = _latest_instant(cash_points)
    if li:
        cash_latest = safe_float(li.get("val"))
        cash_end = li.get("end")
    out["A_Cash_USD"] = cash_latest
    out["A_Cash_End"] = cash_end

    # Debt total
    debt_total = None
    dlt = _latest_instant(debt_lt_points)
    dst = _latest_instant(debt_st_points)
    dltv = safe_float(dlt.get("val")) if dlt else None
    dstv = safe_float(dst.get("val")) if dst else None
    if dltv is not None or dstv is not None:
        debt_total = (dltv or 0.0) + (dstv or 0.0)
    out["A_Total_Debt_USD"] = debt_total

    # Current ratio
    curr_ratio = None
    ca = _latest_instant(curr_assets_points)
    cl = _latest_instant(curr_liab_points)
    cav = safe_float(ca.get("val")) if ca else None
    clv = safe_float(cl.get("val")) if cl else None
    if cav is not None and clv not in (None, 0.0):
        curr_ratio = cav / clv
    out["A_Current_Ratio"] = curr_ratio

    # Debt / equity
    de_ratio = None
    eq = _latest_instant(equity_points)
    eqv = safe_float(eq.get("val")) if eq else None
    if debt_total is not None and eqv not in (None, 0.0):
        de_ratio = debt_total / eqv
    out["A_Debt_to_Equity"] = de_ratio

    # OCF / FCF (TTM-ish)
    ttm_ocf = None
    if len(ocf_points) >= 4:
        ttm_ocf = _sum_vals(_latest_four_quarters(ocf_points))
    else:
        fy = [p for p in ocf_points if p.get("fp") == "FY"]
        if fy:
            ttm_ocf = safe_float(fy[-1].get("val"))
    out["A_TTM_OCF_USD"] = ttm_ocf

    ttm_capex = None
    if len(capex_points) >= 4:
        ttm_capex = _sum_vals(_latest_four_quarters(capex_points))
    else:
        fy = [p for p in capex_points if p.get("fp") == "FY"]
        if fy:
            ttm_capex = safe_float(fy[-1].get("val"))
    out["A_TTM_Capex_USD"] = ttm_capex

    ttm_fcf = None
    if ttm_ocf is not None and ttm_capex is not None:
        ttm_fcf = ttm_ocf - ttm_capex
    out["A_TTM_FCF_USD"] = ttm_fcf

    # Shares outstanding (latest)
    sh = _latest_instant(shares_points)
    out["A_Shares_Outstanding"] = safe_float(sh.get("val")) if sh else None
    out["A_Shares_AsOf"] = sh.get("end") if sh else None

    # SBC (latest period)
    sbc_latest = safe_float(sbc_points[-1].get("val")) if sbc_points else None
    out["A_SBC_USD_Latest"] = sbc_latest

    return out


# -----------------------------
# Tier C: Filing-type signals from submissions
# -----------------------------

def tier_c_metrics(submissions: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    recent = (((submissions or {}).get("filings") or {}).get("recent") or {})
    forms = recent.get("form") or []
    filing_dates = recent.get("filingDate") or []
    accession = recent.get("accessionNumber") or []
    primary_docs = recent.get("primaryDocument") or []

    # Basic counts in windows
    today = date.today()
    d90 = today - timedelta(days=90)
    d365 = today - timedelta(days=365)

    def in_window(i: int, start: date) -> bool:
        if i >= len(filing_dates):
            return False
        dd = parse_date(str(filing_dates[i]))
        return bool(dd and dd >= start)

    def count_form(form_set: set[str], start: date) -> int:
        c = 0
        for i, f in enumerate(forms):
            if f in form_set and in_window(i, start):
                c += 1
        return c

    out["C_10Q_Count_365d"] = count_form({"10-Q"}, d365)
    out["C_10K_Count_365d"] = count_form({"10-K"}, d365)
    out["C_8K_Count_365d"] = count_form({"8-K"}, d365)

    out["C_S1_or_S3_Count_365d"] = count_form({"S-1", "S-1/A", "S-3", "S-3/A"}, d365)
    out["C_424B_Count_365d"] = count_form({"424B1","424B2","424B3","424B4","424B5","424B7"}, d365)

    out["C_13D_13G_Count_365d"] = count_form(
        {"SC 13D","SC 13D/A","SC 13G","SC 13G/A"}, d365
    )

    out["C_Form4_Count_90d"] = count_form({"4","4/A"}, d90)

    # Latest core filing dates
    def latest_date_for(form_set: set[str]) -> Optional[str]:
        best: Optional[date] = None
        for i, f in enumerate(forms):
            if f not in form_set:
                continue
            dd = parse_date(str(filing_dates[i]))
            if dd and (best is None or dd > best):
                best = dd
        return best.isoformat() if best else None

    out["C_Latest_10Q_Date"] = latest_date_for({"10-Q"})
    out["C_Latest_10K_Date"] = latest_date_for({"10-K"})
    out["C_Latest_Shelf_Date"] = latest_date_for({"S-3","S-3/A","S-1","S-1/A"})
    out["C_Latest_Form4_Date"] = latest_date_for({"4","4/A"})

    # Keep references needed for Tier B parsing (best effort)
    # We'll expose the recent list length to help debugging
    out["C_Recent_Filings_Listed"] = len(forms)

    # Store arrays for Tier B (not written to Excel)
    out["_recent_forms"] = forms
    out["_recent_filing_dates"] = filing_dates
    out["_recent_accession"] = accession
    out["_recent_primary_docs"] = primary_docs

    return out


# -----------------------------
# Tier B: Insider Form 4 parsing (best-effort)
# -----------------------------

def _parse_form4_net_open_market(xml_text: str) -> Tuple[Optional[float], Optional[int]]:
    """
    Best-effort Form 4 XML parsing:
    - Sum open market purchases (transactionCode P) as +shares
    - Sum sales (transactionCode S) as -shares
    Returns (net_shares, tx_count)
    """
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return None, None

    # Different Form 4 XML variants exist; we will search broadly.
    net = 0.0
    tx = 0

    def findall_local(tag: str) -> List[ET.Element]:
        # match by localname ignoring namespaces
        out = []
        for el in root.iter():
            if el.tag.split("}")[-1] == tag:
                out.append(el)
        return out

    # Transactions can be in nonDerivativeTransaction / derivativeTransaction
    # We'll focus on non-derivative common stock transactions first.
    for nd in findall_local("nonDerivativeTransaction"):
        code_el = None
        shares_el = None
        for el in nd.iter():
            ln = el.tag.split("}")[-1]
            if ln == "transactionCode":
                code_el = el
            if ln == "transactionShares":
                # often has inner <value>
                shares_el = el
        if code_el is None or shares_el is None:
            continue

        code = "".join((code_el.text or "").split()).upper()
        # Extract shares numeric (look for nested <value>)
        shares_val = None
        val_nodes = [e for e in shares_el.iter() if e.tag.split("}")[-1] == "value"]
        if val_nodes and val_nodes[-1].text:
            shares_val = safe_float(val_nodes[-1].text.strip())
        elif shares_el.text:
            shares_val = safe_float(shares_el.text.strip())

        if shares_val is None:
            continue

        if code == "P":
            net += shares_val
            tx += 1
        elif code == "S":
            net -= shares_val
            tx += 1

    if tx == 0:
        return None, None
    return net, tx

def tier_b_metrics_from_recent_forms(
    cik10: str,
    tier_c: Dict[str, Any],
    *,
    lookback_days: int = 180,
    max_forms_to_check: int = 10,
) -> Dict[str, Any]:
    """
    Uses submissions recent filings (Tier C) to locate recent Form 4 filings,
    downloads their primaryDocument (often XML), and computes net shares best-effort.

    Returns N/A if not available.
    """
    out: Dict[str, Any] = {}

    forms: List[str] = tier_c.get("_recent_forms") or []
    filing_dates: List[str] = tier_c.get("_recent_filing_dates") or []
    accession: List[str] = tier_c.get("_recent_accession") or []
    primary_docs: List[str] = tier_c.get("_recent_primary_docs") or []

    today = date.today()
    start = today - timedelta(days=lookback_days)

    net_total = 0.0
    tx_total = 0
    checked = 0
    last_form4 = None

    cik_nolead = str(int(cik10))  # archives path uses no leading zeros

    for i, f in enumerate(forms):
        if checked >= max_forms_to_check:
            break
        if f not in {"4", "4/A"}:
            continue
        dd = parse_date(str(filing_dates[i])) if i < len(filing_dates) else None
        if not dd or dd < start:
            continue

        acc = accession[i] if i < len(accession) else None
        doc = primary_docs[i] if i < len(primary_docs) else None
        if not acc or not doc:
            continue

        # Build archive URL
        acc_nodash = acc.replace("-", "")
        url = SEC_ARCHIVES_PRIMARYDOC_TMPL.format(
            cik_nolead=cik_nolead,
            acc_nodash=acc_nodash,
            primary_doc=doc,
        )

        xml_text = get_text(url, cache_key=f"form4_{cik10}_{acc_nodash}")
        checked += 1
        if not xml_text:
            continue

        net, tx = _parse_form4_net_open_market(xml_text)
        if net is None or tx is None:
            continue

        net_total += net
        tx_total += tx
        last_form4 = max(last_form4 or dd, dd)

    if tx_total == 0:
        out["B_Form4_Net_Shares_180d"] = None
        out["B_Form4_Tx_Count_180d"] = None
        out["B_Form4_Last_Date_Parsed"] = None
    else:
        out["B_Form4_Net_Shares_180d"] = net_total
        out["B_Form4_Tx_Count_180d"] = tx_total
        out["B_Form4_Last_Date_Parsed"] = last_form4.isoformat() if last_form4 else None

    return out


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
    best_row = None
    best_map: Dict[str, int] = {}

    for r in range(1, max_rows + 1):
        row_map: Dict[str, int] = {}
        for c in range(1, ws.max_column + 1):
            v = ws.cell(row=r, column=c).value
            if isinstance(v, str) and v.strip():
                row_map[normalize_header(v)] = c
        # detect a likely header row
        if "ticker" in row_map and ("cik" in row_map or "company name" in row_map):
            best_row = r
            best_map = row_map
            break

    if best_row is None:
        best_row = 1
        best_map = {}
        for c in range(1, ws.max_column + 1):
            v = ws.cell(row=1, column=c).value
            if isinstance(v, str) and v.strip():
                best_map[normalize_header(v)] = c

    return best_row, best_map

def ensure_columns(ws: Worksheet, header_row: int, header_map: Dict[str, int], columns: List[str]) -> Dict[str, int]:
    """
    Ensure all columns exist. Add missing at the end.
    Returns updated header_map (normalized -> col index).
    """
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
    """
    Map ticker -> row number for existing rows.
    """
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

    # Determine all columns to ensure (base + dynamic keys)
    dynamic_cols: List[str] = []
    for r in rows:
        for k in r.keys():
            if k.startswith("_"):
                continue  # internal
            if k not in dynamic_cols and k not in BASE_HEADERS:
                dynamic_cols.append(k)

    all_cols = BASE_HEADERS + dynamic_cols
    header_map = ensure_columns(ws, header_row, header_map, all_cols)

    ticker_col = header_map.get("ticker")
    if not ticker_col:
        # If no ticker column, create it explicitly
        header_map = ensure_columns(ws, header_row, header_map, ["Ticker"])
        ticker_col = header_map["ticker"]

    existing = build_ticker_row_index(ws, header_row, ticker_col)

    # Find next append row
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
            if k.startswith("_"):
                continue
            col = header_map.get(normalize_header(k))
            if not col:
                # late-add column
                header_map = ensure_columns(ws, header_row, header_map, [k])
                col = header_map[normalize_header(k)]
            ws.cell(row=row_num, column=col).value = na(v)

    wb.save(xlsx_path)


# -----------------------------
# Main orchestration (tiered, safe)
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
        t = t.strip().upper()
        if not t:
            continue

        # Default row if ticker not mapped
        if t not in t2c:
            out_rows.append(build_base_row(
                ticker=t, cik10=None, name=None, sic=None, sic_desc=None, is_ad=None,
                status="Ticker not found in SEC ticker map",
                as_of=as_of,
            ))
            continue

        cik10, name = t2c[t]

        # Wrap each company so failures don’t kill the run
        try:
            submissions = fetch_company_submissions(cik10)
            if not submissions:
                out = build_base_row(
                    ticker=t, cik10=cik10, name=name, sic=None, sic_desc=None, is_ad=None,
                    status="No submissions JSON",
                    as_of=as_of,
                )
                out_rows.append(out)
                continue

            sic, sic_desc = extract_sic(submissions)
            is_ad = (sic in sic_allow) if sic is not None else False

            out = build_base_row(
                ticker=t, cik10=cik10, name=name, sic=sic, sic_desc=sic_desc, is_ad=is_ad,
                status="OK",
                as_of=as_of,
            )

            # Tier C (filing-type signals)
            tier_c: Dict[str, Any] = {}
            if ENABLE_TIER_C:
                try:
                    tier_c = tier_c_metrics(submissions)
                    # write only non-internal keys
                    for k, v in tier_c.items():
                        if not k.startswith("_"):
                            out[k] = v
                except Exception:
                    out["Status"] = "OK (Tier C partial)"

            # Tier A (XBRL/DEI)
            if ENABLE_TIER_A:
                try:
                    facts = fetch_company_facts(cik10)
                    if facts:
                        a = tier_a_metrics(facts)
                        out.update(a)
                    else:
                        out["Status"] = "OK (Tier A missing: companyfacts)"
                except Exception:
                    out["Status"] = "OK (Tier A partial)"

            # Tier B (Form 4 parsing) - best effort
            if ENABLE_TIER_B and tier_c:
                try:
                    b = tier_b_metrics_from_recent_forms(cik10, tier_c)
                    out.update(b)
                except Exception:
                    # Do not downgrade run; just leave N/A fields
                    out["Status"] = out.get("Status") or "OK (Tier B partial)"

            # Optional: add your flag logic as derived fields (examples)
            # Keep these in-script so they never crash if inputs are N/A
            try:
                ttm_rev = safe_float(out.get("A_TTM_Revenue_USD"))
                out["F_TTM_Revenue_lt_500m"] = "TRUE" if (ttm_rev is not None and ttm_rev < 500_000_000) else "FALSE" if ttm_rev is not None else None

                yoy = safe_float(out.get("A_Revenue_YoY_Growth"))
                out["F_YoY_Abs_gt_40pct"] = "TRUE" if (yoy is not None and abs(yoy) > 0.40) else "FALSE" if yoy is not None else None

                opm = safe_float(out.get("A_Operating_Margin"))
                out["F_OpMargin_lt_neg10pct"] = "TRUE" if (opm is not None and opm < -0.10) else "FALSE" if opm is not None else None

                rnd = safe_float(out.get("A_RnD_Intensity"))
                out["F_RnD_gt_20pct"] = "TRUE" if (rnd is not None and rnd > 0.20) else "FALSE" if rnd is not None else None

                sga = safe_float(out.get("A_SGandA_Intensity"))
                out["F_SGandA_gt_50pct"] = "TRUE" if (sga is not None and sga > 0.50) else "FALSE" if sga is not None else None

                cr = safe_float(out.get("A_Current_Ratio"))
                out["F_CurrentRatio_lt_1_5"] = "TRUE" if (cr is not None and cr < 1.5) else "FALSE" if cr is not None else None

                de = safe_float(out.get("A_Debt_to_Equity"))
                out["F_DebtEq_gt_1_0"] = "TRUE" if (de is not None and de > 1.0) else "FALSE" if de is not None else None
            except Exception:
                pass

            out_rows.append(out)

        except Exception:
            out_rows.append(build_base_row(
                ticker=t, cik10=cik10, name=name, sic=None, sic_desc=None, is_ad=None,
                status="Failed (unexpected error in company loop)",
                as_of=as_of,
            ))

    write_company_dicts_to_excel(OUTPUT_XLSX, out_rows)

    print(f"Universe input: {universe_path}")
    print(f"Processed tickers: {len(tickers)}")
    print(f"Updated workbook: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
