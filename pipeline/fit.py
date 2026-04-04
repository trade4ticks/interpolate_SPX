"""
Stage 2 — Smile fitting.

For each expiry:
  - Fit a weighted cubic smoothing spline to w(k) (total variance vs log-moneyness)
  - Smoothing parameter is data-driven: based on spread-derived noise per point
  - Check for butterfly arbitrage (Durrleman condition) on the fitted spline
  - Check for calendar spread arbitrage across all expiries at a snapshot

The spline is stored in the FitResult and consumed by sample.py.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
from scipy.interpolate import UnivariateSpline

from .config import ARB_CHECK_POINTS, BUTTERFLY_TOL, MIN_STRIKES_FOR_FIT


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class FitResult:
    """Outcome of fitting one expiry's smile."""
    spline:            Optional[UnivariateSpline]
    T:                 float
    F:                 float
    r:                 float
    k_min:             float = 0.0
    k_max:             float = 0.0
    rmse:              float = 0.0
    butterfly_arb:     bool  = False
    calendar_arb:      bool  = False   # set later by check_calendar_arb()
    skipped:           bool  = False
    skip_reason:       str   = ""

    def evaluate(self, k: np.ndarray) -> np.ndarray:
        """Evaluate w(k); clamps to spline domain (ext=3 boundary behaviour)."""
        return self.spline(k)

    def is_usable(self) -> bool:
        return not self.skipped and self.spline is not None


# ---------------------------------------------------------------------------
# Smoothing parameter
# ---------------------------------------------------------------------------

def _smoothing_factor(w_noise: np.ndarray) -> float:
    """
    Data-driven smoothing factor s for UnivariateSpline.

    scipy's condition: Σ (w_i * residual_i)² ≤ s,  where w_i = 1/σ_i.
    Setting s = n targets residuals of order 1σ on average (chi-squared mean).
    This respects the per-point noise level: wide-spread wings get more slack.
    """
    return float(len(w_noise))


# ---------------------------------------------------------------------------
# Butterfly arbitrage — Durrleman condition
# ---------------------------------------------------------------------------

def _durrleman_g(k: np.ndarray, w: np.ndarray, dw: np.ndarray, d2w: np.ndarray) -> np.ndarray:
    """
    Durrleman's g(k) ≥ 0 is a necessary condition for no butterfly arbitrage.

    g(k) = (1 - k·w'/(2w))² - (w')²/4·(1/w + 1/4) + w''/2

    Reference: Durrleman (2005), "From implied to spot volatilities".
    """
    w_safe = np.maximum(w, 1e-12)
    term1 = (1.0 - k * dw / (2.0 * w_safe)) ** 2
    term2 = (dw ** 2) / 4.0 * (1.0 / w_safe + 0.25)
    term3 = d2w / 2.0
    return term1 - term2 + term3


def check_butterfly_arb(spline: UnivariateSpline, k_min: float, k_max: float) -> bool:
    """
    Returns True if any butterfly arbitrage is detected on [k_min, k_max].
    """
    k_grid = np.linspace(k_min, k_max, ARB_CHECK_POINTS)
    w   = spline(k_grid)
    dw  = spline.derivative(1)(k_grid)
    d2w = spline.derivative(2)(k_grid)

    # Negative total variance is immediately an arb
    if np.any(w < -1e-8):
        return True

    g = _durrleman_g(k_grid, w, dw, d2w)
    return bool(np.any(g < -BUTTERFLY_TOL))


# ---------------------------------------------------------------------------
# Calendar spread arbitrage
# ---------------------------------------------------------------------------

def check_calendar_arb(fits: list[FitResult]) -> bool:
    """
    Returns True if calendar spread arbitrage is detected across the set of
    expiry fits at a single snapshot.

    Calendar no-arb: w(k, T₁) ≤ w(k, T₂) for all k whenever T₁ < T₂.
    Evaluated on a common k grid covering the intersection of all domains.
    """
    usable = sorted(
        [f for f in fits if f.is_usable()],
        key=lambda f: f.T,
    )
    if len(usable) < 2:
        return False

    k_lo = max(f.k_min for f in usable)
    k_hi = min(f.k_max for f in usable)

    if k_lo >= k_hi:
        return False   # no overlapping domain — can't check

    k_grid = np.linspace(k_lo, k_hi, ARB_CHECK_POINTS)
    w_prev = usable[0].evaluate(k_grid)

    for fit in usable[1:]:
        w_curr = fit.evaluate(k_grid)
        if np.any(w_curr < w_prev - 1e-6):
            return True
        w_prev = w_curr

    return False


# ---------------------------------------------------------------------------
# Smile fitting
# ---------------------------------------------------------------------------

def fit_smile(prepared: dict) -> FitResult:
    """
    Fit a weighted cubic smoothing spline to w(k) for one expiry.

    `prepared` is the dict returned by clean.prepare_expiry().

    Returns a FitResult. Never raises — fitting failures are captured as
    skipped=True with skip_reason set.
    """
    T = prepared["T"]
    F = prepared["F"]
    r = prepared["r"]
    data = prepared["data"]

    k = data["k"].to_numpy(dtype=float)
    w = data["w"].to_numpy(dtype=float)
    noise = data["w_noise"].to_numpy(dtype=float)

    base = FitResult(spline=None, T=T, F=F, r=r)

    if len(k) < MIN_STRIKES_FOR_FIT:
        base.skipped     = True
        base.skip_reason = f"Only {len(k)} k/w points — need ≥ {MIN_STRIKES_FOR_FIT}"
        return base

    # Weights: inverse of noise estimate (larger noise → less weight)
    weights = 1.0 / noise
    s = _smoothing_factor(noise)

    try:
        spline = UnivariateSpline(
            k, w,
            w=weights,
            s=s,
            k=3,    # cubic
            ext=3,  # return boundary value outside domain (no extrapolation noise)
        )
    except Exception as exc:
        base.skipped     = True
        base.skip_reason = f"Spline fit failed: {exc}"
        return base

    # Fit quality
    w_hat = spline(k)
    rmse  = float(np.sqrt(np.mean((w - w_hat) ** 2)))

    # Butterfly arbitrage check
    butterfly_arb = check_butterfly_arb(spline, k[0], k[-1])

    return FitResult(
        spline        = spline,
        T             = T,
        F             = F,
        r             = r,
        k_min         = float(k[0]),
        k_max         = float(k[-1]),
        rmse          = rmse,
        butterfly_arb = butterfly_arb,
    )


# ---------------------------------------------------------------------------
# Annotate calendar arbitrage on a collection of fits (mutates in-place)
# ---------------------------------------------------------------------------

def annotate_calendar_arb(fits: list[FitResult]) -> bool:
    """
    Run the calendar-arb check and set the calendar_arb flag on every fit.
    Returns True if any violation was found.
    """
    arb_detected = check_calendar_arb(fits)
    for f in fits:
        f.calendar_arb = arb_detected
    return arb_detected
