#!/usr/bin/env python3
"""
SEC small-cap screener scraper (gated, tiered, failure-tolerant)

Pipeline per ticker (efficiency-first, with strict “fetch last” for market data):

1) Gate 1 (submissions only): Must be a recent filer (10-K / 10-Q / 20-F).
   - If FAIL: write row + Gate 1 fields, stop.

2) Gate 2 (companyfacts only): Must be “screenable” with tighter EDGAR-only fundamentals.
   - If FAIL: write row + Gate 2 fields, stop.
   - No Yahoo / no market cap / no volume fetched here.

3) Gate 3 (Yahoo fetched LAST, single call):
   - Compute Market Cap (Yahoo last close × EDGAR shares) and require <= $5B
   - Liquidity kill-switch (tight): ADV30 >= 200k shares AND $ADV30 >= $2m
   - If FAIL: write row + Gate 3 fields, stop.

4) Tiers (only for Gate-3 passers):
   - Tier C: filing-type signals (submissions)
   - Tier A: full XBRL/DEI metrics (companyfacts) (reusing fetched facts)
   - Tier B: insider Form 4 parsing (best-effort)

Design goals:
- Never crash the run because one company is missing data.
- Any missing/failed datapoint becomes "N/A" in the output workbook.
- Update existing rows by ticker instead of only appending.
- Recycle fetched data: submissions and companyfacts are fetched once per CIK (disk-cached);
  Yahoo market data is fetched once per ticker per run-day (disk-cached).
"""

from __future__ import annotations

import csv
import json
import re
import time
import zipfile
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
    OUTPUT_XLSX,  # fallback: tickers may be in the Universe sheet of the output workbook
]

UNIVERSE_SHEET = "Universe"
UNIVERSE_TICKER_HEADER = "ticker"  # case-insensitive

# Optional SIC config (not used as a hard gate in this version)
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

# Gate 1 windows (tighter)
G1_MAX_AGE_10K_DAYS = 15 * 30  # ~15 months
G1_MAX_AGE_10Q_DAYS = 6 * 30   # ~6 months
G1_MAX_AGE_20F_DAYS = 18 * 30  # ~18 months

ENABLE_GATE_1 = True
ENABLE_GATE_2 = True
ENABLE_GATE_3 = True

# Gate 2 (tight, EDGAR-only)
G2_MIN_TTM_REVENUE_USD = 25_000_000
G2_MIN_TOTAL_ASSETS_USD = 50_000_000
G2_MIN_CASH_USD = 5_000_000

# Gate 3 (fetched last)
G3_MAX_MARKET_CAP_USD = 5_000_000_000
G3_MIN_ADV30_SHARES = 200_000
G3_MIN_ADV30_DOLLAR = 2_000_000

# Switches: tiers (only run after gates pass)
ENABLE_TIER_A_FULL = True
ENABLE_TIER_B = True   # Form 4 parsing (best-effort)
ENABLE_TIER_C = True


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


def ticker_variants(t: str) -> List[str]:
    """
    Try a small set of common symbol variations to improve SEC ticker map hit-rate.
    """
    t = (t or "").strip().upper()
    if not t:
        return []
    variants = [t]
    variants.append(t.replace(".", "-"))
    variants.append(t.replace("-", "."))
    # Deduplicate preserving order
    out: List[str] = []
    seen = set()
    for x in variants:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


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
# Config: SIC allowlist (optional)
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
    out: List[str] = []
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

def _sec_http_headers() -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
    }


def get_json_cached(url: str, cache_key: str, *, headers: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
    """Fetch JSON with a simple on-disk cache. Returns None on failure."""
    cache_path = CACHE_DIR / f"{cache_key}.json"
    if cache_path.exists() and cache_path.stat().st_size > 10:
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    try:
        resp = requests.get(url, headers=headers or _sec_http_headers(), timeout=HTTP_TIMEOUT)
        sleep_rate_limit()
        resp.raise_for_status()
        data = resp.json()
        cache_path.write_text(json.dumps(data), encoding="utf-8")
        return data
    except Exception:
        return None


def get_text(url: str, cache_key: str) -> Optional[str]:
    """Fetch text (e.g., Form 4 XML) with cache. Returns None on failure."""
    cache_path = CACHE_DIR / f"{cache_key}.txt"
    if cache_path.exists() and cache_path.stat().st_size > 10:
        try:
            return cache_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            pass

    try:
        resp = requests.get(url, headers=_sec_http_headers(), timeout=HTTP_TIMEOUT)
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


def resolve_cik_from_map(ticker: str, t2c: Dict[str, Tuple[str, str]]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns (matched_ticker, cik10, name) or (None, None, None).
    """
    for tv in ticker_variants(ticker):
        if tv in t2c:
            cik10, name = t2c[tv]
            return tv, cik10, name
    return None, None, None


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
# Gate 1: reporting recency (submissions-only)
# -----------------------------

def _latest_date_for_form(submissions: Dict[str, Any], forms_wanted: set[str]) -> Optional[date]:
    recent = (((submissions or {}).get("filings") or {}).get("recent") or {})
    forms = recent.get("form") or []
    filing_dates = recent.get("filingDate") or []

    best: Optional[date] = None
    for i, f in enumerate(forms):
        if f not in forms_wanted:
            continue
        if i >= len(filing_dates):
            continue
        dd = parse_date(str(filing_dates[i]))
        if dd and (best is None or dd > best):
            best = dd
    return best


def gate1_reporting(submissions: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    today = date.today()

    latest_10k = _latest_date_for_form(submissions, {"10-K", "10-K/A"})
    latest_10q = _latest_date_for_form(submissions, {"10-Q", "10-Q/A"})
    latest_20f = _latest_date_for_form(submissions, {"20-F", "20-F/A"})

    pass_10k = bool(latest_10k and (today - latest_10k).days <= G1_MAX_AGE_10K_DAYS)
    pass_10q = bool(latest_10q and (today - latest_10q).days <= G1_MAX_AGE_10Q_DAYS)
    pass_20f = bool(latest_20f and (today - latest_20f).days <= G1_MAX_AGE_20F_DAYS)

    passed = pass_10k or pass_10q or pass_20f

    extras = {
        "G1_Pass_Reporting": "TRUE" if passed else "FALSE",
        "G1_Latest_10K": latest_10k.isoformat() if latest_10k else None,
        "G1_Latest_10Q": latest_10q.isoformat() if latest_10q else None,
        "G1_Latest_20F": latest_20f.isoformat() if latest_20f else None,
    }

    if passed:
        if pass_10k:
            reason = "PASS: Recent 10-K"
        elif pass_10q:
            reason = "PASS: Recent 10-Q"
        else:
            reason = "PASS: Recent 20-F"
    else:
        if latest_10k or latest_10q or latest_20f:
            reason = "FAIL: Filings exist but stale"
        else:
            reason = "FAIL: No 10-K/10-Q/20-F found"

    extras["G1_Reason"] = reason
    return passed, reason, extras


# -----------------------------
# Tier A helpers (XBRL / companyfacts)
# -----------------------------

GAAP_TAGS = {
    "revenue": ["Revenues", "SalesRevenueNet"],
    "op_income": ["OperatingIncomeLoss"],
    "rnd": ["ResearchAndDevelopmentExpense"],
    "sga": ["SellingGeneralAndAdministrativeExpense"],
    "ocf": ["NetCashProvidedByUsedInOperatingActivities"],
    "capex": ["PaymentsToAcquirePropertyPlantAndEquipment"],
    "cash": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ],
    "cash_st_inv": ["CashAndCashEquivalentsAndShortTermInvestments"],
    "debt_lt": ["LongTermDebtNoncurrent", "LongTermDebt"],
    "debt_st": ["DebtCurrent", "LongTermDebtCurrent"],
    "current_assets": ["AssetsCurrent"],
    "current_liab": ["LiabilitiesCurrent"],
    "equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "sbc": ["ShareBasedCompensation"],

    # Added for tight Gate 2
    "net_income": ["NetIncomeLoss"],
    "gross_profit": ["GrossProfit"],
    "assets": ["Assets"],
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
    points.sort(key=lambda x: x.get("end", ""))
    return points


def _latest_instant(points: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return points[-1] if points else None


def _latest_four_quarters(points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return points[-4:] if len(points) > 4 else points


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


# -----------------------------
# Gate 2: minimum screenability (companyfacts-only, tight)
# -----------------------------

def tier_a_min_metrics(companyfacts: Dict[str, Any]) -> Dict[str, Any]:
    """
    Minimum subset needed for Gate 2 (EDGAR-only, tight):
      - A_TTM_Revenue_USD (+ end date)
      - A_Total_Assets_USD (+ end date)
      - A_TTM_OCF_USD
      - A_TTM_NetIncome_USD
      - A_TTM_GrossProfit_USD
      - A_Cash_USD (+ end date)
      - A_Shares_Outstanding (+ as-of)
    """
    out: Dict[str, Any] = {}

    forms = {"10-Q", "10-K"}
    fps_flow = {"Q1", "Q2", "Q3", "FY"}
    fps_inst = {"Q1", "Q2", "Q3", "FY"}

    rev_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["revenue"], forms=forms, fps=fps_flow)
    ocf_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["ocf"], forms=forms, fps=fps_flow)
    ni_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["net_income"], forms=forms, fps=fps_flow)
    gp_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["gross_profit"], forms=forms, fps=fps_flow)

    cash_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["cash_st_inv"], forms=forms, fps=fps_inst)
    if not cash_points:
        cash_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["cash"], forms=forms, fps=fps_inst)

    assets_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["assets"], forms=forms, fps=fps_inst)
    shares_points = _pick_period_points(companyfacts, "dei", DEI_TAGS["shares_out"], forms=forms, fps=fps_inst)

    # TTM-ish revenue
    ttm_rev = None
    ttm_rev_end = None
    if len(rev_points) >= 4:
        last4 = _latest_four_quarters(rev_points)
        ttm_rev = _sum_vals(last4)
        ttm_rev_end = last4[-1].get("end")
    else:
        fy = [p for p in rev_points if p.get("fp") == "FY"]
        if fy:
            last = fy[-1]
            ttm_rev = safe_float(last.get("val"))
            ttm_rev_end = last.get("end")
    out["A_TTM_Revenue_USD"] = ttm_rev
    out["A_TTM_Revenue_End"] = ttm_rev_end

    # TTM-ish OCF
    ttm_ocf = None
    if len(ocf_points) >= 4:
        ttm_ocf = _sum_vals(_latest_four_quarters(ocf_points))
    else:
        fy = [p for p in ocf_points if p.get("fp") == "FY"]
        if fy:
            ttm_ocf = safe_float(fy[-1].get("val"))
    out["A_TTM_OCF_USD"] = ttm_ocf

    # TTM-ish Net Income
    ttm_ni = None
    if len(ni_points) >= 4:
        ttm_ni = _sum_vals(_latest_four_quarters(ni_points))
    else:
        fy = [p for p in ni_points if p.get("fp") == "FY"]
        if fy:
            ttm_ni = safe_float(fy[-1].get("val"))
    out["A_TTM_NetIncome_USD"] = ttm_ni

    # TTM-ish Gross Profit
    ttm_gp = None
    if len(gp_points) >= 4:
        ttm_gp = _sum_vals(_latest_four_quarters(gp_points))
    else:
        fy = [p for p in gp_points if p.get("fp") == "FY"]
        if fy:
            ttm_gp = safe_float(fy[-1].get("val"))
    out["A_TTM_GrossProfit_USD"] = ttm_gp

    # Cash (latest instant)
    cash_latest = None
    cash_end = None
    li = _latest_instant(cash_points)
    if li:
        cash_latest = safe_float(li.get("val"))
        cash_end = li.get("end")
    out["A_Cash_USD"] = cash_latest
    out["A_Cash_End"] = cash_end

    # Total Assets (latest instant)
    assets_latest = None
    assets_end = None
    ai = _latest_instant(assets_points)
    if ai:
        assets_latest = safe_float(ai.get("val"))
        assets_end = ai.get("end")
    out["A_Total_Assets_USD"] = assets_latest
    out["A_Total_Assets_End"] = assets_end

    # Shares outstanding (latest)
    sh = _latest_instant(shares_points)
    out["A_Shares_Outstanding"] = safe_float(sh.get("val")) if sh else None
    out["A_Shares_AsOf"] = sh.get("end") if sh else None

    return out


def gate2_basics(out: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Gate 2 (tight, EDGAR-only):
      - (TTM Revenue >= $25m OR Total Assets >= $50m)
      - Shares outstanding > 0
      - Operating reality exists: at least one of (TTM OCF, TTM Net Income, TTM Gross Profit) is non-null
      - Cash >= $5m OR TTM OCF exists
    """
    rev = safe_float(out.get("A_TTM_Revenue_USD"))
    assets = safe_float(out.get("A_Total_Assets_USD"))
    shares = safe_float(out.get("A_Shares_Outstanding"))

    cash = safe_float(out.get("A_Cash_USD"))
    ocf = safe_float(out.get("A_TTM_OCF_USD"))
    ni = safe_float(out.get("A_TTM_NetIncome_USD"))
    gp = safe_float(out.get("A_TTM_GrossProfit_USD"))

    size_ok = ((rev is not None and rev >= G2_MIN_TTM_REVENUE_USD) or (assets is not None and assets >= G2_MIN_TOTAL_ASSETS_USD))
    shares_ok = (shares is not None and shares > 0)
    operating_exists = (ocf is not None) or (ni is not None) or (gp is not None)
    cash_or_ocf_ok = ((cash is not None and cash >= G2_MIN_CASH_USD) or (ocf is not None))

    passed = size_ok and shares_ok and operating_exists and cash_or_ocf_ok

    if not size_ok:
        reason = f"FAIL: Too small (Rev<{G2_MIN_TTM_REVENUE_USD/1e6:.0f}m and Assets<{G2_MIN_TOTAL_ASSETS_USD/1e6:.0f}m) or missing"
    elif not shares_ok:
        reason = "FAIL: Shares outstanding missing/<=0"
    elif not operating_exists:
        reason = "FAIL: No operating reality metric (OCF/NI/GP all missing)"
    elif not cash_or_ocf_ok:
        reason = f"FAIL: Cash<{G2_MIN_CASH_USD/1e6:.0f}m and OCF missing"
    else:
        reason = "PASS: EDGAR fundamentals screenable (tight)"

    extras = {
        "G2_Pass_Basics": "TRUE" if passed else "FALSE",
        "G2_Reason": reason,
        "G2_Size_OK": "TRUE" if size_ok else "FALSE",
        "G2_Shares_OK": "TRUE" if shares_ok else "FALSE",
        "G2_Operating_Exists": "TRUE" if operating_exists else "FALSE",
        "G2_CashOrOCF_OK": "TRUE" if cash_or_ocf_ok else "FALSE",
    }
    return passed, reason, extras


# -----------------------------
# Gate 3: Market cap + liquidity (Yahoo fetched last, single call)
# -----------------------------

def yahoo_quote_and_liquidity(ticker: str, *, adv_days: int = 30, lookback_days: int = 120) -> Dict[str, Any]:
    """
    One Yahoo chart call (disk-cached per ticker per day):
      - last_close, last_date
      - ADV over last adv_days trading days (shares)
      - $ADV over last adv_days trading days (close*volume)
    """
    yahoo_ticker = ticker.replace(".", "-").upper()
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=lookback_days)

    period1 = int(time.mktime(start.timetuple()))
    period2 = int(time.mktime((end + timedelta(days=1)).timetuple()))

    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_ticker}"
        f"?period1={period1}&period2={period2}&interval=1d"
    )

    # Cache key includes date so repeated runs same day do not re-fetch
    cache_key = f"yahoo_chart_{yahoo_ticker}_{end.isoformat()}"
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}

    j = get_json_cached(url, cache_key=cache_key, headers=headers)
    if not j:
        return {"P_Close_Price_USD": None, "P_Price_Date": None, "ADV30_Shares": None, "ADV30_Dollar": None}

    result = (j.get("chart") or {}).get("result") or []
    if not result:
        return {"P_Close_Price_USD": None, "P_Price_Date": None, "ADV30_Shares": None, "ADV30_Dollar": None}

    res0 = result[0]
    ts = res0.get("timestamp") or []
    quote0 = (((res0.get("indicators") or {}).get("quote") or [{}])[0])
    closes = quote0.get("close") or []
    vols = quote0.get("volume") or []

    rows: List[Tuple[date, float, float]] = []
    for tstamp, c, v in zip(ts, closes, vols):
        if c is None or v is None:
            continue
        d = datetime.fromtimestamp(int(tstamp), tz=timezone.utc).date()
        rows.append((d, float(c), float(v)))

    if not rows:
        return {"P_Close_Price_USD": None, "P_Price_Date": None, "ADV30_Shares": None, "ADV30_Dollar": None}

    rows.sort(key=lambda x: x[0])
    last_d, last_c, _ = rows[-1]

    tail = rows[-adv_days:] if len(rows) >= adv_days else rows
    adv_sh = (sum(r[2] for r in tail) / len(tail)) if tail else None
    adv_dl = (sum((r[1] * r[2]) for r in tail) / len(tail)) if tail else None

    return {
        "P_Close_Price_USD": last_c,
        "P_Price_Date": last_d.isoformat(),
        "ADV30_Shares": adv_sh,
        "ADV30_Dollar": adv_dl,
    }


def gate3_marketcap_and_liquidity(out: Dict[str, Any], ticker: str) -> Tuple[bool, str, Dict[str, Any]]:
    shares = safe_float(out.get("A_Shares_Outstanding"))
    q = yahoo_quote_and_liquidity(ticker)

    price = safe_float(q.get("P_Close_Price_USD"))
    adv_sh = safe_float(q.get("ADV30_Shares"))
    adv_dl = safe_float(q.get("ADV30_Dollar"))

    mcap = None
    if shares is not None and price is not None:
        mcap = shares * price

    mcap_ok = (mcap is not None and mcap <= G3_MAX_MARKET_CAP_USD)
    liq_ok = (
        adv_sh is not None and adv_sh >= G3_MIN_ADV30_SHARES
        and adv_dl is not None and adv_dl >= G3_MIN_ADV30_DOLLAR
    )

    passed = mcap_ok and liq_ok

    if mcap is None:
        reason = "FAIL: Could not compute market cap (missing price or shares)"
    elif not mcap_ok:
        reason = f"FAIL: Market cap > ${G3_MAX_MARKET_CAP_USD:,.0f}"
    elif not liq_ok:
        reason = f"FAIL: Illiquid (ADV30<{G3_MIN_ADV30_SHARES:,} or $ADV30<${G3_MIN_ADV30_DOLLAR:,.0f})"
    else:
        reason = "PASS: Market cap + liquidity (tight)"

    extras = {
        "G3_Pass": "TRUE" if passed else "FALSE",
        "G3_Reason": reason,
        "P_Close_Price_USD": q.get("P_Close_Price_USD"),
        "P_Price_Date": q.get("P_Price_Date"),
        "P_Market_Cap_USD": mcap,
        "P_MarketCap_Method": "Yahoo Last Close × EDGAR Shares" if mcap is not None else None,
        "P_ADV30_Shares": adv_sh,
        "P_ADV30_Dollar": adv_dl,
    }
    return passed, reason, extras


# -----------------------------
# Tier A: FULL metrics (reuses facts)
# -----------------------------

def tier_a_full_metrics(companyfacts: Dict[str, Any]) -> Dict[str, Any]:
    """
    Full Tier A metrics (best-effort). Market cap handled in Gate 3.
    """
    out: Dict[str, Any] = {}

    forms = {"10-Q", "10-K"}
    fps_flow = {"Q1", "Q2", "Q3", "FY"}
    fps_inst = {"Q1", "Q2", "Q3", "FY"}

    rev_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["revenue"], forms=forms, fps=fps_flow)
    opinc_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["op_income"], forms=forms, fps=fps_flow)
    rnd_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["rnd"], forms=forms, fps=fps_flow)
    sga_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["sga"], forms=forms, fps=fps_flow)
    ocf_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["ocf"], forms=forms, fps=fps_flow)
    capex_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["capex"], forms=forms, fps=fps_flow)
    sbc_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["sbc"], forms=forms, fps=fps_flow)

    cash_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["cash_st_inv"], forms=forms, fps=fps_inst)
    if not cash_points:
        cash_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["cash"], forms=forms, fps=fps_inst)

    debt_lt_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["debt_lt"], forms=forms, fps=fps_inst)
    debt_st_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["debt_st"], forms=forms, fps=fps_inst)
    curr_assets_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["current_assets"], forms=forms, fps=fps_inst)
    curr_liab_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["current_liab"], forms=forms, fps=fps_inst)
    equity_points = _pick_period_points(companyfacts, "us-gaap", GAAP_TAGS["equity"], forms=forms, fps=fps_inst)

    shares_points = _pick_period_points(companyfacts, "dei", DEI_TAGS["shares_out"], forms=forms, fps=fps_inst)

    # TTM revenue and YoY growth
    ttm_rev = None
    ttm_rev_end = None
    if len(rev_points) >= 4:
        last4 = _latest_four_quarters(rev_points)
        ttm_rev = _sum_vals(last4)
        ttm_rev_end = last4[-1].get("end")
    else:
        fy = [p for p in rev_points if p.get("fp") == "FY"]
        if fy:
            last = fy[-1]
            ttm_rev = safe_float(last.get("val"))
            ttm_rev_end = last.get("end")
    out["A_TTM_Revenue_USD"] = ttm_rev
    out["A_TTM_Revenue_End"] = ttm_rev_end

    yoy = None
    if len(rev_points) >= 8:
        latest = rev_points[-1]
        prev_year = rev_points[-5]
        yoy = _yoy_growth(safe_float(latest.get("val")), safe_float(prev_year.get("val")))
    else:
        fy = [p for p in rev_points if p.get("fp") == "FY"]
        if len(fy) >= 2:
            yoy = _yoy_growth(safe_float(fy[-1].get("val")), safe_float(fy[-2].get("val")))
    out["A_Revenue_YoY_Growth"] = yoy

    # Operating margin (latest period)
    op_margin = None
    if rev_points and opinc_points:
        rev_last = safe_float(rev_points[-1].get("val"))
        op_last = safe_float(opinc_points[-1].get("val"))
        if rev_last and rev_last != 0 and op_last is not None:
            op_margin = op_last / rev_last
    out["A_Operating_Margin"] = op_margin

    # Intensities
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

    # Cash (latest)
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

    # SBC latest
    out["A_SBC_USD_Latest"] = safe_float(sbc_points[-1].get("val")) if sbc_points else None

    return out


# -----------------------------
# Tier C: filing-type signals from submissions
# -----------------------------

def tier_c_metrics(submissions: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    recent = (((submissions or {}).get("filings") or {}).get("recent") or {})
    forms = recent.get("form") or []
    filing_dates = recent.get("filingDate") or []
    accession = recent.get("accessionNumber") or []
    primary_docs = recent.get("primaryDocument") or []

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
    out["C_424B_Count_365d"] = count_form({"424B1", "424B2", "424B3", "424B4", "424B5", "424B7"}, d365)
    out["C_13D_13G_Count_365d"] = count_form({"SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"}, d365)
    out["C_Form4_Count_90d"] = count_form({"4", "4/A"}, d90)

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
    out["C_Latest_Shelf_Date"] = latest_date_for({"S-3", "S-3/A", "S-1", "S-1/A"})
    out["C_Latest_Form4_Date"] = latest_date_for({"4", "4/A"})
    out["C_Recent_Filings_Listed"] = len(forms)

    # Internal arrays for Tier B
    out["_recent_forms"] = forms
    out["_recent_filing_dates"] = filing_dates
    out["_recent_accession"] = accession
    out["_recent_primary_docs"] = primary_docs

    return out


# -----------------------------
# Tier B: Insider Form 4 parsing (best-effort)
# -----------------------------

def _parse_form4_net_open_market(xml_text: str) -> Tuple[Optional[float], Optional[int]]:
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return None, None

    net = 0.0
    tx = 0

    def findall_local(tag: str) -> List[ET.Element]:
        out: List[ET.Element] = []
        for el in root.iter():
            if el.tag.split("}")[-1] == tag:
                out.append(el)
        return out

    for nd in findall_local("nonDerivativeTransaction"):
        code_el = None
        shares_el = None
        for el in nd.iter():
            ln = el.tag.split("}")[-1]
            if ln == "transactionCode":
                code_el = el
            if ln == "transactionShares":
                shares_el = el
        if code_el is None or shares_el is None:
            continue

        code = "".join((code_el.text or "").split()).upper()
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
    last_form4: Optional[date] = None

    cik_nolead = str(int(cik10))

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

    # Gate 1
    "G1_Pass_Reporting",
    "G1_Reason",
    "G1_Latest_10K",
    "G1_Latest_10Q",
    "G1_Latest_20F",

    # Gate 2 (tight, EDGAR-only)
    "G2_Pass_Basics",
    "G2_Reason",
    "G2_Size_OK",
    "G2_Shares_OK",
    "G2_Operating_Exists",
    "G2_CashOrOCF_OK",

    # Gate 3 (fetched last)
    "G3_Pass",
    "G3_Reason",

    # Price / liquidity / market cap (computed in Gate 3)
    "P_Close_Price_USD",
    "P_Price_Date",
    "P_Market_Cap_USD",
    "P_MarketCap_Method",
    "P_ADV30_Shares",
    "P_ADV30_Dollar",

    # Min EDGAR fields (useful to see why excluded)
    "A_TTM_Revenue_USD",
    "A_TTM_Revenue_End",
    "A_Total_Assets_USD",
    "A_Total_Assets_End",
    "A_TTM_OCF_USD",
    "A_TTM_NetIncome_USD",
    "A_TTM_GrossProfit_USD",
    "A_Cash_USD",
    "A_Cash_End",
    "A_Shares_Outstanding",
    "A_Shares_AsOf",
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
            if k.startswith("_"):
                continue
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
            if k.startswith("_"):
                continue
            col = header_map.get(normalize_header(k))
            if not col:
                header_map = ensure_columns(ws, header_row, header_map, [k])
                col = header_map[normalize_header(k)]
            ws.cell(row=row_num, column=col).value = na(v)

    wb.save(xlsx_path)


# -----------------------------
# Main orchestration
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
        t = (t or "").strip().upper()
        if not t:
            continue

        matched, cik10, name = resolve_cik_from_map(t, t2c)

        # --- Ticker not mapped: immediate stop ---
        if not cik10:
            out = build_base_row(
                ticker=t,
                cik10=None,
                name=None,
                sic=None,
                sic_desc=None,
                is_ad=None,
                status="Ticker not found in SEC ticker map",
                as_of=as_of,
            )
            out.update({
                "G1_Pass_Reporting": "FALSE",
                "G1_Reason": "FAIL: Not in SEC ticker map",
                "G1_Latest_10K": None,
                "G1_Latest_10Q": None,
                "G1_Latest_20F": None,

                "G2_Pass_Basics": "FALSE",
                "G2_Reason": "FAIL: Gate 1 failed (no CIK)",
                "G3_Pass": "FALSE",
                "G3_Reason": "FAIL: Gate 1 failed (no CIK)",
            })
            out_rows.append(out)
            continue

        try:
            # --- Gate 1 inputs: submissions JSON ---
            submissions = fetch_company_submissions(cik10)
            if not submissions:
                out = build_base_row(
                    ticker=t,
                    cik10=cik10,
                    name=name,
                    sic=None,
                    sic_desc=None,
                    is_ad=None,
                    status="No submissions JSON",
                    as_of=as_of,
                )
                out.update({
                    "G1_Pass_Reporting": "FALSE",
                    "G1_Reason": "FAIL: No submissions JSON",
                    "G1_Latest_10K": None,
                    "G1_Latest_10Q": None,
                    "G1_Latest_20F": None,

                    "G2_Pass_Basics": "FALSE",
                    "G2_Reason": "FAIL: Gate 1 failed (no submissions)",
                    "G3_Pass": "FALSE",
                    "G3_Reason": "FAIL: Gate 1 failed (no submissions)",
                })
                out_rows.append(out)
                continue

            sic, sic_desc = extract_sic(submissions)
            is_ad = (sic in sic_allow) if sic is not None else False

            out = build_base_row(
                ticker=t,
                cik10=cik10,
                name=name,
                sic=sic,
                sic_desc=sic_desc,
                is_ad=is_ad,
                status="OK",
                as_of=as_of,
            )

            # -----------------------------
            # Gate 1
            # -----------------------------
            if ENABLE_GATE_1:
                g1_passed, _, g1_fields = gate1_reporting(submissions)
                out.update(g1_fields)
                if not g1_passed:
                    out["Status"] = "Excluded (Gate 1)"
                    out.update({
                        "G2_Pass_Basics": "FALSE",
                        "G2_Reason": "FAIL: Gate 1 failed",
                        "G3_Pass": "FALSE",
                        "G3_Reason": "FAIL: Gate 1 failed",
                    })
                    out_rows.append(out)
                    continue

            # -----------------------------
            # Gate 2 (EDGAR-only, tight)
            # -----------------------------
            facts = fetch_company_facts(cik10)
            if not facts:
                out["Status"] = "Excluded (Gate 2)"
                out.update({
                    "G2_Pass_Basics": "FALSE",
                    "G2_Reason": "FAIL: Missing companyfacts",
                    "G3_Pass": "FALSE",
                    "G3_Reason": "FAIL: Gate 2 failed",
                })
                out_rows.append(out)
                continue

            try:
                out.update(tier_a_min_metrics(facts))
            except Exception:
                out["Status"] = "Excluded (Gate 2)"
                out.update({
                    "G2_Pass_Basics": "FALSE",
                    "G2_Reason": "FAIL: Could not parse minimum fundamentals",
                    "G3_Pass": "FALSE",
                    "G3_Reason": "FAIL: Gate 2 failed",
                })
                out_rows.append(out)
                continue

            g2_passed, _, g2_fields = gate2_basics(out)
            out.update(g2_fields)

            if ENABLE_GATE_2 and not g2_passed:
                out["Status"] = "Excluded (Gate 2)"
                out.update({
                    "G3_Pass": "FALSE",
                    "G3_Reason": "FAIL: Gate 2 failed",
                    # Ensure no market fields for excluded Gate 2
                    "P_Close_Price_USD": None,
                    "P_Price_Date": None,
                    "P_Market_Cap_USD": None,
                    "P_MarketCap_Method": None,
                    "P_ADV30_Shares": None,
                    "P_ADV30_Dollar": None,
                })
                out_rows.append(out)
                continue

            # -----------------------------
            # Gate 3 (Yahoo fetched LAST, tight)
            # -----------------------------
            if ENABLE_GATE_3:
                g3_passed, _, g3_fields = gate3_marketcap_and_liquidity(out, t)
                out.update(g3_fields)
                if not g3_passed:
                    out["Status"] = "Excluded (Gate 3)"
                    out_rows.append(out)
                    continue

            # -----------------------------
            # Tiers (only for Gate-3 passers)
            # -----------------------------

            tier_c: Dict[str, Any] = {}
            if ENABLE_TIER_C:
                try:
                    tier_c = tier_c_metrics(submissions)
                    for k, v in tier_c.items():
                        if not k.startswith("_"):
                            out[k] = v
                except Exception:
                    out["Status"] = "OK (Tier C partial)"

            if ENABLE_TIER_A_FULL:
                try:
                    out.update(tier_a_full_metrics(facts))
                except Exception:
                    out["Status"] = "OK (Tier A full partial)"

            if ENABLE_TIER_B and tier_c:
                try:
                    b = tier_b_metrics_from_recent_forms(cik10, tier_c)
                    out.update(b)
                except Exception:
                    out["Status"] = out.get("Status") or "OK (Tier B partial)"

            # Example derived flags (kept, only after gates)
            try:
                ttm_rev2 = safe_float(out.get("A_TTM_Revenue_USD"))
                out["F_TTM_Revenue_lt_500m"] = (
                    "TRUE" if (ttm_rev2 is not None and ttm_rev2 < 500_000_000)
                    else "FALSE" if ttm_rev2 is not None else None
                )

                yoy = safe_float(out.get("A_Revenue_YoY_Growth"))
                out["F_YoY_Abs_gt_40pct"] = (
                    "TRUE" if (yoy is not None and abs(yoy) > 0.40)
                    else "FALSE" if yoy is not None else None
                )

                opm = safe_float(out.get("A_Operating_Margin"))
                out["F_OpMargin_lt_neg10pct"] = (
                    "TRUE" if (opm is not None and opm < -0.10)
                    else "FALSE" if opm is not None else None
                )

                rnd = safe_float(out.get("A_RnD_Intensity"))
                out["F_RnD_gt_20pct"] = (
                    "TRUE" if (rnd is not None and rnd > 0.20)
                    else "FALSE" if rnd is not None else None
                )

                sga = safe_float(out.get("A_SGandA_Intensity"))
                out["F_SGandA_gt_50pct"] = (
                    "TRUE" if (sga is not None and sga > 0.50)
                    else "FALSE" if sga is not None else None
                )

                cr = safe_float(out.get("A_Current_Ratio"))
                out["F_CurrentRatio_lt_1_5"] = (
                    "TRUE" if (cr is not None and cr < 1.5)
                    else "FALSE" if cr is not None else None
                )

                de = safe_float(out.get("A_Debt_to_Equity"))
                out["F_DebtEq_gt_1_0"] = (
                    "TRUE" if (de is not None and de > 1.0)
                    else "FALSE" if de is not None else None
                )
            except Exception:
                pass

            out_rows.append(out)

        except Exception:
            # Hard-fail safe row
            out = build_base_row(
                ticker=t,
                cik10=cik10,
                name=name,
                sic=None,
                sic_desc=None,
                is_ad=None,
                status="Failed (unexpected error in company loop)",
                as_of=as_of,
            )
            out.update({
                "G1_Pass_Reporting": "FALSE",
                "G1_Reason": "FAIL: Unexpected error",
                "G2_Pass_Basics": "FALSE",
                "G2_Reason": "FAIL: Unexpected error",
                "G3_Pass": "FALSE",
                "G3_Reason": "FAIL: Unexpected error",
            })
            out_rows.append(out)

    write_company_dicts_to_excel(OUTPUT_XLSX, out_rows)

    print(f"Universe input: {universe_path}")
    print(f"Processed tickers: {len(tickers)}")
    print(f"Updated workbook: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
