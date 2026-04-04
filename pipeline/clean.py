"""
Stage 1 — Cleaning and preparation.

For each expiry at a given snapshot timestamp:
  - Compute T (time to expiry in years using calendar minutes)
  - Filter illiquid / bad quotes
  - Estimate forward price F and risk-free rate r via put-call parity OLS
  - Compute log-moneyness k = ln(K/F) and total variance w = IV² × T
  - Estimate per-point noise in w-space from bid/ask spreads
"""
from __future__ import annotations

import math
import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from scipy.stats import linregress

logger = logging.getLogger(__name__)

from .config import (
    AM_EXPIRY_HOUR, AM_EXPIRY_MINUTE,
    COLS, MAX_IV, MAX_SPREAD_RATIO, MIN_BID, MIN_IV,
    MIN_OPTION_PRICE, MIN_STRIKES_FOR_FIT, MINUTES_PER_YEAR,
    PCP_MONEYNESS_BAND, PM_EXPIRY_HOUR, PM_EXPIRY_MINUTE,
    R_DEFAULT, R_MAX, R_MIN, STEP2_FLAG_COLS,
)

ET = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Time-to-expiry
# ---------------------------------------------------------------------------

def compute_T(snapshot_ts: pd.Timestamp, expiry_date: pd.Timestamp, is_am: bool) -> float:
    """
    Return T = time-to-expiry in years using calendar minutes.
    Uses actual minutes remaining rather than fractional days so that
    0DTE Greeks (gamma, delta) reflect the true short time horizon.
    """
    exp_time = time(AM_EXPIRY_HOUR, AM_EXPIRY_MINUTE) if is_am \
               else time(PM_EXPIRY_HOUR, PM_EXPIRY_MINUTE)

    expiry_naive = datetime.combine(expiry_date.date(), exp_time)

    # Normalise snapshot to naive ET
    if getattr(snapshot_ts, "tzinfo", None) is not None:
        snap_naive = snapshot_ts.tz_convert(ET).replace(tzinfo=None)
    else:
        snap_naive = snapshot_ts.to_pydatetime().replace(tzinfo=None)

    minutes = (expiry_naive - snap_naive).total_seconds() / 60.0
    return max(minutes, 0.0) / MINUTES_PER_YEAR


# ---------------------------------------------------------------------------
# Quote filtering
# ---------------------------------------------------------------------------

def _normalise_option_type(series: pd.Series) -> pd.Series:
    """Return lowercase first character: 'p' or 'c'."""
    return series.astype(str).str.strip().str.lower().str[0]


def filter_quotes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove illiquid and problematic quotes.
    Also drops any rows flagged by step-2 quality columns if they exist.
    """
    c = COLS
    mask = pd.Series(True, index=df.index)

    # Minimum bid
    mask &= df[c["bid"]] >= MIN_BID

    # No crossed / inverted markets
    mask &= df[c["ask"]] > df[c["bid"]]

    # Spread sanity: (ask - bid) / bid ≤ MAX_SPREAD_RATIO
    mask &= (df[c["ask"]] - df[c["bid"]]) / df[c["bid"]] <= MAX_SPREAD_RATIO

    # IV bounds
    mask &= df[c["iv"]] >= MIN_IV
    mask &= df[c["iv"]] <= MAX_IV

    # Honour any step-2 flag columns that exist
    for flag_col in STEP2_FLAG_COLS:
        if flag_col in df.columns:
            mask &= ~df[flag_col].astype(bool)

    return df[mask].copy()


# ---------------------------------------------------------------------------
# Forward price and risk-free rate via put-call parity
# ---------------------------------------------------------------------------

def compute_forward_rate(df: pd.DataFrame, T: float) -> tuple[float, float]:
    """
    Estimate F (forward price) and r (risk-free rate) from put-call parity.

    C_mid - P_mid = e^(-rT) * (F - K)
    Rearranged as a linear system:
        C_mid - P_mid = A - B * K
    where A = e^(-rT)*F and B = e^(-rT).

    Uses only near-ATM strikes (within PCP_MONEYNESS_BAND of a rough ATM
    estimate) to avoid deep-ITM pairs with poor mid-price accuracy.

    Returns (F, r).
    Raises ValueError if regression fails or results are implausible.
    """
    c = COLS
    df = df.copy()
    df["_type"] = _normalise_option_type(df[c["option_type"]])
    df["_mid"] = (df[c["bid"]] + df[c["ask"]]) / 2.0

    calls = (
        df[df["_type"] == "c"]
        .set_index(c["strike"])[["_mid"]]
        .rename(columns={"_mid": "call_mid"})
    )
    puts = (
        df[df["_type"] == "p"]
        .set_index(c["strike"])[["_mid"]]
        .rename(columns={"_mid": "put_mid"})
    )

    pairs = calls.join(puts, how="inner").dropna()

    if len(pairs) < 3:
        raise ValueError(
            f"Only {len(pairs)} matched put-call pairs — need ≥ 3 for PCP regression"
        )

    # Drop pairs where either mid is below minimum price (deep ITM noise)
    pairs = pairs[(pairs["call_mid"] >= MIN_OPTION_PRICE) &
                  (pairs["put_mid"]  >= MIN_OPTION_PRICE)]

    if len(pairs) < 3:
        raise ValueError(
            "Fewer than 3 usable put-call pairs after price filter"
        )

    K = pairs.index.to_numpy(dtype=float)
    y = (pairs["call_mid"] - pairs["put_mid"]).to_numpy(dtype=float)  # C - P

    # Rough ATM estimate as the strike minimising |C - P|
    atm_guess = K[np.argmin(np.abs(y))]

    # Restrict to near-ATM band
    band_mask = np.abs(K / atm_guess - 1.0) <= PCP_MONEYNESS_BAND
    if band_mask.sum() < 3:
        band_mask = np.ones(len(K), dtype=bool)  # fall back to all pairs

    K_fit = K[band_mask]
    y_fit = y[band_mask]

    result = linregress(K_fit, y_fit)
    B = -result.slope      # e^(-rT)
    A = result.intercept   # e^(-rT) * F

    if B <= 0:
        raise ValueError(f"PCP regression yielded non-positive discount factor (B={B:.6f})")

    r = -np.log(B) / T
    F = A / B

    if F <= 0:
        raise ValueError(f"PCP regression yielded non-positive forward price (F={F:.2f})")

    if not (R_MIN <= r <= R_MAX):
        raise ValueError(
            f"Implied risk-free rate out of plausible range: r={r:.4f}"
        )

    return float(F), float(r)


# ---------------------------------------------------------------------------
# Log-moneyness and total variance
# ---------------------------------------------------------------------------

def compute_surface_inputs(df: pd.DataFrame, F: float, T: float) -> pd.DataFrame:
    """
    Compute k = ln(K/F) and w = IV² * T for each clean quote.
    Use OTM options only:
      - puts  for K ≤ F  (k ≤ 0)
      - calls for K ≥ F  (k ≥ 0)
      - small overlap zone around ATM (|k| ≤ 0.01) → average of put and call IV

    Also computes w_noise: spread-based noise estimate for the smoothing spline.

    Returns a DataFrame with columns [k, w, w_noise] sorted by k,
    de-duplicated (one row per unique k value).
    """
    c = COLS
    df = df.copy()
    df["_type"] = _normalise_option_type(df[c["option_type"]])
    df["k"] = np.log(df[c["strike"]].astype(float) / F)
    df["w"] = df[c["iv"]].astype(float) ** 2 * T

    # Noise in w-space: δw = 2·IV·T·δIV, where δIV ≈ spread / (2·vega)
    # The step-2 parquet includes the raw vega column — use it directly.
    # Fallback to a bid/ask-fraction proxy if the column is absent.
    spread = df[c["ask"]] - df[c["bid"]]
    if "vega" in df.columns:
        vega_vals = df["vega"].abs().clip(lower=1e-6)
        df["w_noise"] = (df[c["iv"]].astype(float) * T * spread / vega_vals).clip(lower=1e-8)
    else:
        mid = (df[c["bid"]] + df[c["ask"]]) / 2.0
        spread_frac = spread / mid.clip(lower=1e-4)
        df["w_noise"] = (df["w"] * spread_frac).clip(lower=1e-8)

    # OTM selection with a small ATM overlap band
    atm_band = 0.01
    put_mask  = (df["_type"] == "p") & (df["k"] <=  atm_band)
    call_mask = (df["_type"] == "c") & (df["k"] >= -atm_band)
    selected  = pd.concat([df[put_mask], df[call_mask]], ignore_index=True)

    # Average duplicates in the overlap zone (same strike from both put and call)
    agg = (
        selected
        .groupby("k", sort=True)[["w", "w_noise"]]
        .mean()
        .reset_index()
    )

    return agg[["k", "w", "w_noise"]].copy()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def prepare_expiry(
    df: pd.DataFrame,
    snapshot_ts: pd.Timestamp,
    expiry_date: pd.Timestamp,
    is_am: bool,
) -> dict:
    """
    Full cleaning pipeline for one expiry at one snapshot timestamp.

    Returns a dict:
        T         float   — time to expiry in years
        F         float   — forward price
        r         float   — risk-free rate (annualised, continuous)
        data      DataFrame — columns [k, w, w_noise], sorted by k
        n_raw     int
        n_clean   int

    Raises ValueError with a descriptive message if the expiry cannot be
    processed (expired, too few quotes, bad PCP regression, etc.).
    """
    T = compute_T(snapshot_ts, expiry_date, is_am)
    if T <= 0:
        raise ValueError("Expiry is already in the past at this snapshot")

    clean = filter_quotes(df)
    n_raw   = len(df)
    n_clean = len(clean)

    if n_clean < MIN_STRIKES_FOR_FIT:
        raise ValueError(
            f"Only {n_clean} clean quotes — need ≥ {MIN_STRIKES_FOR_FIT}"
        )

    try:
        F, r = compute_forward_rate(clean, T)
    except ValueError as exc:
        # For short-dated expiries (small T) the PCP regression slope is
        # indistinguishable from -1 and noise dominates the rate estimate.
        # Fall back: use underlying_price as an approximate spot price and
        # derive F with a default rate. The carry error is < 0.1% for T < 0.03.
        up_col = COLS["underlying_price"]
        if up_col not in clean.columns or clean[up_col].isna().all():
            raise
        S = float(clean[up_col].median())
        r = R_DEFAULT
        F = S * math.exp(r * T)
        logger.debug(
            "PCP fallback (T=%.4f): using underlying_price F=%.2f r=%.4f — %s",
            T, F, r, exc,
        )

    surface_df = compute_surface_inputs(clean, F, T)

    if len(surface_df) < MIN_STRIKES_FOR_FIT:
        raise ValueError(
            f"Only {len(surface_df)} usable k/w points after OTM selection"
        )

    return {
        "T":       T,
        "F":       F,
        "r":       r,
        "data":    surface_df,
        "n_raw":   n_raw,
        "n_clean": n_clean,
    }
