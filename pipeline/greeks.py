"""
Stage 4 — Greek calculation.

Computes Black-Scholes Greeks at each sampled (DTE, put_delta) point using
the smoothed IV from sample.py.

Convention: forward greeks, with F as the underlying.
  d1 = (ln(F/K) + 0.5·σ²·T) / (σ·√T)  ≡  (-k + 0.5·w) / √w
  d2 = d1 - σ·√T                         ≡  d1 - √w

  Put price = e^(-rT) · [K·N(-d2) - F·N(-d1)]
  Vega      = e^(-rT) · F · N'(d1) · √T          (∂V/∂σ, same for put and call)
  Gamma     = e^(-rT) · N'(d1) / (F · σ · √T)    (∂²V/∂F², forward gamma)
  Theta     = −[e^(-rT)·F·N'(d1)·σ/(2√T) − r·PV(K)·N(-d2) + r·PV(F)·N(-d1)] / 365
              expressed per calendar day

All greeks are stored for the put at each delta node.
For the ITM put side (put_delta 55-95, k > 0), the price is the ITM put price
even though the IV was derived from the OTM call quote — put-call parity holds.
"""
from __future__ import annotations

import math

import numpy as np
from scipy.stats import norm

_SQRT_2PI = math.sqrt(2.0 * math.pi)


def _d1_d2(k: float, w: float) -> tuple[float, float]:
    """
    k = ln(K/F)  (log-moneyness, negative for OTM puts)
    w = σ²·T     (total variance)
    """
    sqrt_w = math.sqrt(max(w, 1e-12))
    d1 = (-k + 0.5 * w) / sqrt_w
    d2 = d1 - sqrt_w
    return d1, d2


def _nprime(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / _SQRT_2PI


def bs_put_greeks(F: float, K: float, T: float, r: float, sigma: float) -> dict:
    """
    Black-Scholes put price and forward greeks.

    Parameters
    ----------
    F     : forward price
    K     : strike
    T     : time to expiry in years
    r     : risk-free rate (annualised, continuous)
    sigma : implied volatility (annualised)

    Returns
    -------
    dict with keys: price, theta, vega, gamma
    """
    if sigma <= 0 or T <= 0:
        return {"price": None, "theta": None, "vega": None, "gamma": None}

    w   = sigma * sigma * T
    k   = math.log(K / F)
    d1, d2 = _d1_d2(k, w)

    disc  = math.exp(-r * T)
    Nd1   = norm.cdf(d1)
    Nd2   = norm.cdf(d2)
    Nnd1  = norm.cdf(-d1)
    Nnd2  = norm.cdf(-d2)
    npd1  = _nprime(d1)
    sqrt_T = math.sqrt(T)

    # Put price
    price = disc * (K * Nnd2 - F * Nnd1)

    # Vega: ∂V/∂σ  (same formula for put and call)
    vega  = disc * F * npd1 * sqrt_T

    # Forward Gamma: ∂²V/∂F²
    gamma = disc * npd1 / (F * sigma * sqrt_T)

    # Theta: ∂V/∂t per calendar day (note: put theta can be positive for deep ITM)
    theta_per_year = (
        - disc * F * npd1 * sigma / (2.0 * sqrt_T)
        + r * disc * K * Nnd2
        - r * disc * F * Nnd1
    )
    theta = theta_per_year / 365.0   # per calendar day

    return {
        "price": price,
        "theta": theta,
        "vega":  vega,
        "gamma": gamma,
    }


def enrich_surface_rows(surface_rows: list[dict]) -> list[dict]:
    """
    Add price, theta, vega, gamma to each row produced by sample.sample_surface().

    Each row already contains: dte, put_delta, iv, k, strike, F, r, T.
    Returns the same list with greek fields added in-place.
    """
    for row in surface_rows:
        greeks = bs_put_greeks(
            F     = row["F"],
            K     = row["strike"],
            T     = row["T"],
            r     = row["r"],
            sigma = row["iv"],
        )
        row.update(greeks)

    return surface_rows
