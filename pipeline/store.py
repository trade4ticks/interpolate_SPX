"""
Stage 5 — PostgreSQL storage.

Handles:
  - Schema initialisation (run sql/schema.sql once)
  - Monthly partition creation on demand (via stored functions)
  - Upsert of surface, ATM, and diagnostics rows
"""
from __future__ import annotations

import logging
import math
from pathlib import Path

import numpy as np
import psycopg2
import psycopg2.extras

from .config import DB_URL

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# NumPy scalar sanitisation
# ---------------------------------------------------------------------------

def _sanitize_row(row: dict) -> dict:
    """
    Convert numpy scalars to Python-native types before passing to psycopg2.

    NumPy >= 2.0 changed repr(np.float64(x)) to 'np.float64(x)' instead of
    just 'x'. psycopg2 falls back to repr() for unregistered types, which
    causes PostgreSQL to see literal text like 'np.float64(2.46)' and fail
    with 'schema "np" does not exist'.

    Also maps NaN and Inf to None (→ NULL) to avoid invalid SQL literals.
    """
    result = {}
    for k, v in row.items():
        if isinstance(v, np.floating):
            f = float(v)
            result[k] = None if not math.isfinite(f) else f
        elif isinstance(v, np.integer):
            result[k] = int(v)
        elif isinstance(v, np.bool_):
            result[k] = bool(v)
        elif isinstance(v, float) and not math.isfinite(v):
            result[k] = None
        else:
            result[k] = v
    return result

_SCHEMA_PATH = Path(__file__).parent.parent / "sql" / "schema.sql"


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(DB_URL)


# ---------------------------------------------------------------------------
# One-time schema initialisation
# ---------------------------------------------------------------------------

def init_db() -> None:
    """
    Create tables, indexes, and partition helper functions if they don't exist.
    Safe to call on an already-initialised database.
    """
    sql = _SCHEMA_PATH.read_text()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    logger.info("Schema initialised (or already current)")


# ---------------------------------------------------------------------------
# Partition management
# ---------------------------------------------------------------------------

def ensure_partitions(conn: psycopg2.extensions.connection, trade_date: str) -> None:
    """
    Call the DB-side helper functions to create monthly partitions for
    spx_surface and spx_atm if they don't already exist.
    trade_date: ISO string 'YYYY-MM-DD'
    """
    with conn.cursor() as cur:
        cur.execute("SELECT ensure_surface_partition(%s::DATE)", (trade_date,))
        cur.execute("SELECT ensure_atm_partition(%s::DATE)",     (trade_date,))
    conn.commit()


# ---------------------------------------------------------------------------
# Surface upsert
# ---------------------------------------------------------------------------

_SURFACE_UPSERT = """
INSERT INTO spx_surface
    (trade_date, quote_time, dte, put_delta, iv, price, theta, vega, gamma)
VALUES
    (%(trade_date)s, %(quote_time)s, %(dte)s, %(put_delta)s,
     %(iv)s, %(price)s, %(theta)s, %(vega)s, %(gamma)s)
ON CONFLICT (trade_date, quote_time, dte, put_delta)
DO UPDATE SET
    iv    = EXCLUDED.iv,
    price = EXCLUDED.price,
    theta = EXCLUDED.theta,
    vega  = EXCLUDED.vega,
    gamma = EXCLUDED.gamma
"""


def upsert_surface(
    conn: psycopg2.extensions.connection,
    rows: list[dict],
) -> None:
    """
    Bulk-upsert rows into spx_surface.
    Each row must have: trade_date, quote_time, dte, put_delta, iv,
                        price, theta, vega, gamma.
    """
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur, _SURFACE_UPSERT, [_sanitize_row(r) for r in rows], page_size=1000
        )
    conn.commit()


# ---------------------------------------------------------------------------
# ATM upsert
# ---------------------------------------------------------------------------

_ATM_UPSERT = """
INSERT INTO spx_atm
    (trade_date, quote_time, dte,
     atm_put_delta, atm_strike, atm_iv, atm_forward,
     price, theta, vega, gamma)
VALUES
    (%(trade_date)s, %(quote_time)s, %(dte)s,
     %(atm_put_delta)s, %(atm_strike)s, %(atm_iv)s, %(atm_forward)s,
     %(price)s, %(theta)s, %(vega)s, %(gamma)s)
ON CONFLICT (trade_date, quote_time, dte)
DO UPDATE SET
    atm_put_delta = EXCLUDED.atm_put_delta,
    atm_strike    = EXCLUDED.atm_strike,
    atm_iv        = EXCLUDED.atm_iv,
    atm_forward   = EXCLUDED.atm_forward,
    price         = EXCLUDED.price,
    theta         = EXCLUDED.theta,
    vega          = EXCLUDED.vega,
    gamma         = EXCLUDED.gamma
"""


def upsert_atm(
    conn: psycopg2.extensions.connection,
    rows: list[dict],
) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur, _ATM_UPSERT, [_sanitize_row(r) for r in rows], page_size=500
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Diagnostics upsert
# ---------------------------------------------------------------------------

_DIAG_UPSERT = """
INSERT INTO spx_surface_diagnostics (
    trade_date, quote_time, expiry, expiry_type,
    dte_actual, forward_price, risk_free_rate,
    n_strikes_raw, n_strikes_clean,
    spline_rmse, calendar_arb_flag, butterfly_arb_flag,
    skipped, skip_reason
) VALUES (
    %(trade_date)s, %(quote_time)s, %(expiry)s, %(expiry_type)s,
    %(dte_actual)s, %(forward_price)s, %(risk_free_rate)s,
    %(n_strikes_raw)s, %(n_strikes_clean)s,
    %(spline_rmse)s, %(calendar_arb_flag)s, %(butterfly_arb_flag)s,
    %(skipped)s, %(skip_reason)s
)
ON CONFLICT (trade_date, quote_time, expiry)
DO UPDATE SET
    dte_actual         = EXCLUDED.dte_actual,
    forward_price      = EXCLUDED.forward_price,
    risk_free_rate     = EXCLUDED.risk_free_rate,
    n_strikes_raw      = EXCLUDED.n_strikes_raw,
    n_strikes_clean    = EXCLUDED.n_strikes_clean,
    spline_rmse        = EXCLUDED.spline_rmse,
    calendar_arb_flag  = EXCLUDED.calendar_arb_flag,
    butterfly_arb_flag = EXCLUDED.butterfly_arb_flag,
    skipped            = EXCLUDED.skipped,
    skip_reason        = EXCLUDED.skip_reason
"""


def upsert_diagnostics(
    conn: psycopg2.extensions.connection,
    rows: list[dict],
) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur, _DIAG_UPSERT, [_sanitize_row(r) for r in rows], page_size=500
        )
    conn.commit()
