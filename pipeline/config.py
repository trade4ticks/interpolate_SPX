"""
Pipeline configuration.

All tuneable constants live here. Override DATA_ROOT and DB_URL via environment
variables (or a .env file at the project root).
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Paths and database
# ---------------------------------------------------------------------------
# Comma-separated list of data roots to search for trade-date parquet folders.
# The first root that contains a matching YYYYMMDD directory wins.
DATA_ROOTS: list[Path] = [
    Path(p.strip())
    for p in os.environ.get(
        "SPX_DATA_ROOTS",
        os.environ.get("SPX_DATA_ROOT", "/mnt/volume1/spx_options"),
    ).split(",")
]

# Backwards compat: single DATA_ROOT still used in a few places — points to first entry.
DATA_ROOT = DATA_ROOTS[0]

DB_URL = os.environ.get("SPX_DB_URL", "postgresql://user:password@localhost:5432/spx")

# ---------------------------------------------------------------------------
# Surface grid
# ---------------------------------------------------------------------------
TARGET_DTES: list[int] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14, 21, 30, 45, 60, 90, 120, 180, 270, 360
]

# Integer put deltas 5-95 (unified convention).
# 5  → |Δ_put| = 0.05 (deep OTM put)
# 95 → |Δ_put| = 0.95 (deep ITM put, derived from OTM call)
TARGET_DELTAS: list[int] = list(range(5, 100, 5))

# ---------------------------------------------------------------------------
# Parquet column names — matched to the step-2 (clean_SPX) output schema
# ---------------------------------------------------------------------------
COLS: dict[str, str] = {
    "trade_date":       "trade_date",        # object / str  "2026-01-02"
    "quote_time":       "quote_time",        # object / str  "09:35:00"
    "strike":           "strike",            # float64
    "option_type":      "right",             # str: 'C' or 'P'
    "settlement":       "settlement",        # str: 'AM' or 'PM'
    "bid":              "bid",               # float64
    "ask":              "ask",               # float64
    "iv":               "implied_vol",       # float64, decimal (0.25 = 25%)
    "underlying_price": "underlying_price",  # float64 — SPX spot price
    "dte":              "dte",               # int64 — calendar DTE from step 2
}

# Step-2 flag columns — rows where any of these are True are dropped in clean.py.
# Already computed by clean_SPX; no need to recheck the same conditions.
STEP2_FLAG_COLS: list[str] = [
    "flag_crossed_market",
    "flag_zero_bid",
    "flag_negative_extrinsic",
    "flag_iv_missing",
    "flag_iv_extreme_high",
    "flag_iv_extreme_low",
]

# ---------------------------------------------------------------------------
# Expiry settlement times (Eastern, 24h clock)
# ---------------------------------------------------------------------------
PM_EXPIRY_HOUR:   int = 16
PM_EXPIRY_MINUTE: int = 15   # SPX PM settlement
AM_EXPIRY_HOUR:   int = 9
AM_EXPIRY_MINUTE: int = 30   # SPX AM (opening print) settlement

MINUTES_PER_YEAR: float = 365.0 * 24.0 * 60.0

# ---------------------------------------------------------------------------
# Quote filtering thresholds
# ---------------------------------------------------------------------------
MIN_BID: float = 0.05          # drop quotes with bid below this
MAX_SPREAD_RATIO: float = 5.0  # drop if (ask-bid)/bid > this
MIN_IV: float = 0.01           # 1%  — drop implausibly low IV
MAX_IV: float = 5.00           # 500% — drop implausibly high IV
MIN_OPTION_PRICE: float = 0.05 # drop matched pairs with mid below this

# ---------------------------------------------------------------------------
# PCP regression: only use strikes within this fraction of F for F/r estimate
# Avoids deep-ITM pairs that have wide spreads and poor mid-price accuracy.
# ---------------------------------------------------------------------------
PCP_MONEYNESS_BAND: float = 0.15  # use strikes within ±15% of F

# Sanity bounds for implied risk-free rate (annualised, continuous)
R_MIN: float = -0.05  # -5%
R_MAX: float = 0.20   # 20%

# Fallback rate used when PCP regression is unreliable (e.g. very short-dated
# expiries where T is tiny and the slope is indistinguishable from -1).
# Used together with underlying_price to form an approximate forward F.
R_DEFAULT: float = 0.05

# ---------------------------------------------------------------------------
# Spline fitting
# ---------------------------------------------------------------------------
MIN_STRIKES_FOR_FIT: int = 5   # skip expiry if fewer clean strikes remain

# Durrleman butterfly-arb check tolerance (allow tiny negative g values from
# numerical noise)
BUTTERFLY_TOL: float = 1e-4

# Number of k-grid points used for arbitrage checks
ARB_CHECK_POINTS: int = 200

# ---------------------------------------------------------------------------
# Delta solver (brentq)
# ---------------------------------------------------------------------------
DELTA_SOLVER_K_BOUNDS: tuple[float, float] = (-4.0, 4.0)  # log-moneyness search range
DELTA_SOLVER_XTOL: float = 1e-8
