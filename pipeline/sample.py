"""
Stage 3 — Sampling the production grid.

For each target DTE:
  1. Find the pair of PM expiries that bracket it in time (skip if no bracket).
  2. Build an interpolated w(k) function via linear interpolation in total
     variance-time space:
       w_target(k) = w_lo(k) + α * (w_hi(k) - w_lo(k))
       where α = (T_target - T_lo) / (T_hi - T_lo)
  3. For each target put delta (5-95), solve for the log-moneyness k that gives
     that forward delta, then convert to IV and strike.
  4. Find the true ATM point (where forward put delta = -0.5, i.e., d1 = 0).

Delta convention: unified put delta expressed as positive integers 5-95.
  |Δ_put| = put_delta / 100
  Forward put delta = N(d1) - 1
  d1 in terms of k and w: d1 = (-k + 0.5*w) / sqrt(w)

All calculations use forward F and rate r from the bracketing expiries
(linearly interpolated to the target T).
"""
from __future__ import annotations

from typing import Optional

import numpy as np
from scipy.optimize import brentq
from scipy.stats import norm

from .config import (
    DELTA_SOLVER_K_BOUNDS, DELTA_SOLVER_XTOL,
    TARGET_DELTAS, TARGET_DTES,
)
from .fit import FitResult


# ---------------------------------------------------------------------------
# Interpolated smile at a target T
# ---------------------------------------------------------------------------

class DirectSmile:
    """
    Wraps a single FitResult for use at 0 DTE (same-day expiry).
    The 0 DTE target can't be bracketed from below, so we use the nearest
    expiry's fitted smile directly rather than interpolating in time.
    target_T is the actual T of that expiry at this snapshot — it changes
    throughout the trading day as expiry approaches.
    """

    def __init__(self, fit: FitResult) -> None:
        self.fit      = fit
        self.target_T = fit.T
        self.F        = fit.F
        self.r        = fit.r
        self.k_min    = fit.k_min
        self.k_max    = fit.k_max

    def w(self, k: float | np.ndarray) -> float | np.ndarray:
        arr = np.atleast_1d(np.asarray(k, dtype=float))
        result = np.maximum(self.fit.evaluate(arr), 1e-12)
        return float(result[0]) if np.ndim(k) == 0 else result

    def iv(self, k: float) -> float:
        w_val = self.w(k)
        return float(np.sqrt(max(w_val / self.target_T, 1e-12)))

    def strike(self, k: float) -> float:
        return float(self.F * np.exp(k))


class InterpolatedSmile:
    """
    Linear interpolation in total-variance space between two expiry fits.
    Evaluates w(k) at the target T by blending the two boundary splines.
    """

    def __init__(self, lo: FitResult, hi: FitResult, target_T: float) -> None:
        if hi.T <= lo.T:
            raise ValueError("hi.T must be > lo.T")
        self.lo       = lo
        self.hi       = hi
        self.target_T = target_T
        self.alpha    = (target_T - lo.T) / (hi.T - lo.T)

        # Usable domain: intersection of both splines
        self.k_min = max(lo.k_min, hi.k_min)
        self.k_max = min(lo.k_max, hi.k_max)

        # Blended forward and rate
        self.F = lo.F + self.alpha * (hi.F - lo.F)
        self.r = lo.r + self.alpha * (hi.r - lo.r)

    def w(self, k: float | np.ndarray) -> float | np.ndarray:
        """Total variance at log-moneyness k."""
        w_lo = self.lo.evaluate(np.atleast_1d(np.asarray(k, dtype=float)))
        w_hi = self.hi.evaluate(np.atleast_1d(np.asarray(k, dtype=float)))
        result = w_lo + self.alpha * (w_hi - w_lo)
        result = np.maximum(result, 1e-12)   # total variance must be non-negative
        return float(result[0]) if np.ndim(k) == 0 else result

    def iv(self, k: float) -> float:
        """Implied volatility at log-moneyness k."""
        w_val = self.w(k)
        return float(np.sqrt(max(w_val / self.target_T, 1e-12)))

    def strike(self, k: float) -> float:
        """Strike corresponding to log-moneyness k."""
        return float(self.F * np.exp(k))


# ---------------------------------------------------------------------------
# Delta ↔ log-moneyness relationship
# ---------------------------------------------------------------------------

def _forward_put_delta(k: float, w: float) -> float:
    """
    Forward put delta given log-moneyness k and total variance w.
    Δ_put = N(d1) - 1,  d1 = (-k + 0.5*w) / sqrt(w)
    """
    if w <= 0:
        return -1.0 if k > 0 else 0.0
    d1 = (-k + 0.5 * w) / np.sqrt(w)
    return float(norm.cdf(d1) - 1.0)


def _delta_residual(k: float, smile: InterpolatedSmile, target_abs_delta: float) -> float:
    """
    Residual for brentq: |Δ_put(k)| - target_abs_delta.
    target_abs_delta ∈ (0, 1).
    """
    w_val = smile.w(float(k))
    delta = _forward_put_delta(float(k), float(w_val))
    return abs(delta) - target_abs_delta


# ---------------------------------------------------------------------------
# Solving for the delta grid
# ---------------------------------------------------------------------------

def solve_delta_grid(
    smile: InterpolatedSmile,
    target_deltas: list[int] = TARGET_DELTAS,
) -> dict[int, dict]:
    """
    For each target put delta (integer 5-95), solve for:
        k   — log-moneyness
        iv  — implied volatility
        K   — strike price

    Returns a dict keyed by integer delta:
        { 25: {"k": -0.12, "iv": 0.18, "strike": 4850.0}, ... }

    Deltas that cannot be solved (e.g., because the target falls outside the
    spline domain) are omitted from the result.
    """
    results: dict[int, dict] = {}
    k_lo, k_hi = DELTA_SOLVER_K_BOUNDS

    for delta_int in target_deltas:
        target_abs = delta_int / 100.0

        try:
            # Sign of residual at bounds
            r_lo = _delta_residual(k_lo, smile, target_abs)
            r_hi = _delta_residual(k_hi, smile, target_abs)

            # If same sign, no root in the bracket — target delta out of range
            if r_lo * r_hi > 0:
                continue

            k_sol = brentq(
                _delta_residual,
                k_lo, k_hi,
                args=(smile, target_abs),
                xtol=DELTA_SOLVER_XTOL,
                full_output=False,
            )
        except ValueError:
            continue

        iv_sol = smile.iv(k_sol)
        K_sol  = smile.strike(k_sol)

        results[delta_int] = {
            "k":      float(k_sol),
            "iv":     float(iv_sol),
            "strike": float(K_sol),
        }

    return results


# ---------------------------------------------------------------------------
# True ATM point
# ---------------------------------------------------------------------------

def find_atm(smile: InterpolatedSmile) -> Optional[dict]:
    """
    Find the true ATM point where the forward put delta equals exactly -0.5,
    i.e., d1 = 0, i.e., k = 0.5 * w(k).

    This is a fixed-point equation solved with brentq on:
        f(k) = k - 0.5 * w(k)  == 0   (rearranged from d1 = 0)

    Returns a dict:
        { "atm_put_delta": float, "atm_strike": float,
          "atm_iv": float, "atm_forward": float }
    or None if the ATM cannot be bracketed.
    """
    def residual(k: float) -> float:
        w_val = smile.w(k)
        return k - 0.5 * float(w_val)

    k_lo, k_hi = DELTA_SOLVER_K_BOUNDS

    try:
        if residual(k_lo) * residual(k_hi) > 0:
            return None

        k_atm = brentq(residual, k_lo, k_hi, xtol=DELTA_SOLVER_XTOL)
    except ValueError:
        return None

    w_atm   = smile.w(k_atm)
    iv_atm  = smile.iv(k_atm)
    K_atm   = smile.strike(k_atm)
    d1_atm  = (-k_atm + 0.5 * w_atm) / max(np.sqrt(w_atm), 1e-12)
    delta_atm = float(norm.cdf(d1_atm) - 1.0)  # should be ≈ -0.5

    return {
        "atm_put_delta": delta_atm,
        "atm_strike":    float(K_atm),
        "atm_iv":        float(iv_atm),
        "atm_forward":   float(smile.F),
    }


# ---------------------------------------------------------------------------
# DTE interpolation — main entry point
# ---------------------------------------------------------------------------

def sample_surface(
    fits: list[FitResult],
    target_dtes: list[int] = TARGET_DTES,
    target_deltas: list[int] = TARGET_DELTAS,
) -> tuple[list[dict], list[dict]]:
    """
    Build the sampled surface for all target DTEs.

    Only PM expiries are used for interpolation (AM expiries were already
    excluded upstream in run.py when building the fits list).

    For each target DTE:
      - Find the two PM fits that bracket it in T-space
      - Skip if no bracket exists (no extrapolation)
      - Build an InterpolatedSmile and solve the delta/ATM grids

    Returns:
        surface_rows  — list of dicts for spx_surface
        atm_rows      — list of dicts for spx_atm
    """
    # Work only with usable fits, sorted by T ascending
    usable = sorted([f for f in fits if f.is_usable()], key=lambda f: f.T)

    if len(usable) < 2:
        return [], []

    surface_rows: list[dict] = []
    atm_rows:     list[dict] = []

    for dte_int in target_dtes:

        # 0 DTE: use the nearest (same-day) expiry directly — no time interpolation.
        # Its actual T changes throughout the day as expiry approaches.
        if dte_int == 0:
            smile: DirectSmile | InterpolatedSmile = DirectSmile(usable[0])
            target_T = smile.target_T
        else:
            target_T = dte_int / 365.0   # calendar days convention

            # Find bracketing pair
            lo: Optional[FitResult] = None
            hi: Optional[FitResult] = None

            for i in range(len(usable) - 1):
                if usable[i].T <= target_T <= usable[i + 1].T:
                    lo = usable[i]
                    hi = usable[i + 1]
                    break

            if lo is None or hi is None:
                continue   # no bracket — skip this DTE

            smile = InterpolatedSmile(lo, hi, target_T)

        # Delta grid
        delta_results = solve_delta_grid(smile, target_deltas)
        for delta_int, pts in delta_results.items():
            surface_rows.append({
                "dte":      dte_int,
                "put_delta": delta_int,
                "iv":        pts["iv"],
                "k":         pts["k"],
                "strike":    pts["strike"],
                "F":         smile.F,
                "r":         smile.r,
                "T":         target_T,
            })

        # ATM point
        atm = find_atm(smile)
        if atm is not None:
            atm_rows.append({
                "dte":           dte_int,
                "atm_put_delta": atm["atm_put_delta"],
                "atm_strike":    atm["atm_strike"],
                "atm_iv":        atm["atm_iv"],
                "atm_forward":   atm["atm_forward"],
                "_r":            smile.r,
                "_T":            target_T,
            })

    return surface_rows, atm_rows
