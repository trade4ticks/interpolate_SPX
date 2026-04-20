"""
Pipeline entry point.

File discovery (folders use YYYYMMDD format, no dashes — matches step-1/2 output):
    DATA_ROOTS[n]/
        YYYYMMDD/             ← trade_date
            YYYYMMDD/         ← expiry
                PM.parquet    ← session (SPXW weeklies / PM-settled monthlies)
                AM.parquet    (SPX monthlies — occasional)

Each parquet file covers one (trade_date, expiry, session) combination but
contains rows for multiple intraday timestamps.

Processing flow per trade_date:
  1. Load all parquet files → one combined DataFrame tagged with expiry + session
  2. Group by timestamp → process each snapshot independently
  3. Per snapshot:
     a. For every expiry (all sessions): clean + fit → FitResult + diagnostics row
     b. Calendar-arb check across PM fits
     c. sample_surface() using PM fits only
     d. Compute Greeks
     e. Write surface, ATM, diagnostics to PostgreSQL

Batch mode:   process a date range
Incremental:  find the latest trade_date already in the DB, process everything after

Usage:
    python -m pipeline.run batch  --start 2024-01-01 --end 2024-12-31
    python -m pipeline.run incremental
    python -m pipeline.run init-db
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2

from .clean import prepare_expiry
from .config import COLS, DATA_ROOT, DATA_ROOTS, TARGET_DELTAS, TARGET_DTES

_UNDERLYING_COL = COLS["underlying_price"]
from .fit import FitResult, annotate_calendar_arb, fit_smile
from .greeks import enrich_atm_rows, enrich_surface_rows
from .sample import sample_surface
from .store import (
    ensure_partitions, get_connection, init_db,
    upsert_atm, upsert_diagnostics, upsert_surface,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def discover_trade_date(trade_date: date) -> list[dict]:
    """
    Scan DATA_ROOTS for <YYYYMMDD>/ and return a list of dicts:
        { "expiry": date, "session": "AM"|"PM", "path": Path }

    Searches each root in DATA_ROOTS in order; returns results from the
    first root that contains a matching trade-date directory.

    Folder names use compact YYYYMMDD format (no dashes), matching the
    step-1 (Thetadata_Raw_SPX) and step-2 (clean_SPX) output layout.
    """
    date_str = trade_date.strftime("%Y%m%d")

    trade_dir = None
    for root in DATA_ROOTS:
        candidate = root / date_str
        if candidate.is_dir():
            trade_dir = candidate
            break

    if trade_dir is None:
        return []

    entries = []
    for expiry_dir in sorted(trade_dir.iterdir()):
        if not expiry_dir.is_dir():
            continue
        try:
            expiry = datetime.strptime(expiry_dir.name, "%Y%m%d").date()
        except ValueError:
            continue   # skip non-date folders

        for session in ("PM", "AM"):
            path = expiry_dir / f"{session}.parquet"
            if path.exists():
                entries.append({"expiry": expiry, "session": session, "path": path})

    return entries


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def load_trade_date(entries: list[dict]) -> pd.DataFrame:
    """
    Load all parquet files for a trade_date and tag each row with
    `_expiry` (Timestamp) and `_session` (str).

    _session is taken from the parquet's `settlement` column when present
    (most reliable), falling back to the filename-derived value.
    Returns a combined DataFrame.
    """
    frames = []
    for entry in entries:
        df = load_parquet(entry["path"])
        df["_expiry"] = pd.Timestamp(entry["expiry"])

        # Prefer the in-data settlement column (already computed by step 2)
        settlement_col = COLS.get("settlement", "settlement")
        if settlement_col in df.columns:
            df["_session"] = df[settlement_col].astype(str).str.upper().str.strip()
        else:
            df["_session"] = entry["session"]

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Build a proper datetime from the already-split trade_date + quote_time columns.
    # Stored as "_ts" to avoid collision with any existing "timestamp" column.
    date_col = COLS["trade_date"]
    time_col = COLS["quote_time"]
    combined["_ts"] = pd.to_datetime(
        combined[date_col].astype(str) + " " + combined[time_col].astype(str)
    )

    return combined


# ---------------------------------------------------------------------------
# Per-snapshot processing
# ---------------------------------------------------------------------------

def _build_diag_row(
    trade_date: date,
    quote_time,
    expiry: date,
    session: str,
    fit: FitResult,
    n_raw: int,
    n_clean: int,
) -> dict:
    return {
        "trade_date":         trade_date.isoformat(),
        "quote_time":         quote_time,
        "expiry":             expiry.isoformat(),
        "expiry_type":        session,
        "dte_actual":         fit.T * 365.0,
        "forward_price":      fit.F if not fit.skipped else None,
        "risk_free_rate":     fit.r if not fit.skipped else None,
        "n_strikes_raw":      n_raw,
        "n_strikes_clean":    n_clean,
        "spline_rmse":        fit.rmse if not fit.skipped else None,
        "calendar_arb_flag":  fit.calendar_arb,
        "butterfly_arb_flag": fit.butterfly_arb,
        "skipped":            fit.skipped,
        "skip_reason":        fit.skip_reason or None,
    }


def process_snapshot(
    snapshot_df: pd.DataFrame,
    snapshot_ts: pd.Timestamp,
    trade_date: date,
    min_expiry_dte: int | None = None,
    target_dtes: list[int] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Process all expiries at a single snapshot timestamp.

    AM expiries are admitted to the sampling pool only when no PM expiry
    exists on the same calendar date. This avoids the same-date "different
    clock" conflict on the front of the curve while picking up the AM-only
    LEAPS that extend the long tail.

    `min_expiry_dte` (optional): skip fitting any (expiry, session) group
    whose calendar DTE from trade_date is below this value. Used by the
    long-DTE backfill so we don't waste time fitting front-month expiries
    that aren't needed to bracket the targeted long DTEs.

    `target_dtes` (optional): override TARGET_DTES for this snapshot.

    Returns:
        surface_rows  — for spx_surface (with Greeks added)
        atm_rows      — for spx_atm
        diag_rows     — for spx_surface_diagnostics
    """
    diag_rows:      list[dict]      = []
    sampling_fits:  list[FitResult] = []
    all_fits:       list[tuple]     = []   # (fit, expiry, session, n_raw, n_clean)

    # Determine which expiry dates have a PM session present in this snapshot
    pm_expiry_dates = {
        expiry_ts.date()
        for (expiry_ts, session), _ in snapshot_df.groupby(["_expiry", "_session"])
        if session == "PM"
    }

    # Group by expiry+session and fit each independently
    for (expiry_ts, session), group in snapshot_df.groupby(["_expiry", "_session"]):
        expiry = expiry_ts.date()
        is_am  = (session == "AM")

        # Optional DTE filter (used by long-DTE backfill)
        if min_expiry_dte is not None:
            if (expiry - trade_date).days < min_expiry_dte:
                continue

        try:
            prepared = prepare_expiry(group, snapshot_ts, expiry_ts, is_am)
            fit      = fit_smile(prepared)
            n_raw    = prepared["n_raw"]
            n_clean  = prepared["n_clean"]
        except ValueError as exc:
            # Build a skipped FitResult so we still write a diagnostics row
            df_len = len(group)
            fit    = FitResult(
                spline=None, T=0.0, F=0.0, r=0.0,
                skipped=True, skip_reason=str(exc),
            )
            n_raw = n_clean = df_len

        all_fits.append((fit, expiry, session, n_raw, n_clean))

        # Sampling pool: PM always; AM only when no same-date PM exists.
        if not fit.skipped:
            if session == "PM":
                sampling_fits.append(fit)
            elif session == "AM" and expiry not in pm_expiry_dates:
                sampling_fits.append(fit)

    # Calendar-arb check across the sampling pool (annotates each fit in-place)
    annotate_calendar_arb(sampling_fits)

    quote_time = snapshot_ts.time()

    # Spot underlying at this snapshot (same across all expiries for a given ts)
    underlying_price: float | None = None
    if _UNDERLYING_COL in snapshot_df.columns:
        up_series = snapshot_df[_UNDERLYING_COL].dropna()
        if not up_series.empty:
            underlying_price = float(up_series.median())

    # Build diagnostics rows for every expiry
    for fit, expiry, session, n_raw, n_clean in all_fits:
        diag_rows.append(
            _build_diag_row(trade_date, quote_time, expiry, session,
                            fit, n_raw, n_clean)
        )

    # Sample surface using the merged sampling pool (PM + non-overlapping AM)
    surface_rows, atm_rows = sample_surface(
        sampling_fits,
        target_dtes=target_dtes if target_dtes is not None else TARGET_DTES,
        target_deltas=TARGET_DELTAS,
    )

    # quote_time already set above when building diag_rows
    for row in surface_rows:
        row["trade_date"] = trade_date.isoformat()
        row["quote_time"] = quote_time

    for row in atm_rows:
        row["trade_date"]       = trade_date.isoformat()
        row["quote_time"]       = quote_time
        row["underlying_price"] = underlying_price

    # Compute Greeks in-place
    enrich_surface_rows(surface_rows)
    enrich_atm_rows(atm_rows)

    # Promote internal F → forward for storage
    for row in surface_rows:
        row["forward"] = row.get("F")

    # Strip internal-only fields before storage
    _surface_keep = {"trade_date", "quote_time", "dte", "put_delta",
                     "iv", "strike", "forward",
                     "price", "theta", "vega", "gamma"}
    surface_rows = [{k: v for k, v in r.items() if k in _surface_keep}
                    for r in surface_rows]

    _atm_keep = {"trade_date", "quote_time", "dte",
                 "atm_put_delta", "atm_strike", "atm_iv", "atm_forward",
                 "underlying_price",
                 "price", "theta", "vega", "gamma"}
    atm_rows = [{k: v for k, v in r.items() if k in _atm_keep}
                for r in atm_rows]

    return surface_rows, atm_rows, diag_rows


# ---------------------------------------------------------------------------
# Per-date processing
# ---------------------------------------------------------------------------

def process_date(
    trade_date: date,
    conn: psycopg2.extensions.connection,
    atm_only: bool = False,
    diag_counts_by_qt=None,
    min_expiry_dte: int | None = None,
    target_dtes: list[int] | None = None,
    write_diagnostics: bool = True,
) -> int:
    """
    Load all data for trade_date, iterate over snapshots, and write results.

    If `diag_counts_by_qt` is provided (mapping `time` -> int count of
    diagnostics rows already in the DB for that quote_time), a snapshot is
    skipped only when the count matches the number of (expiry, session)
    groups currently present in the parquet. This lets the intraday cron
    avoid reprocessing complete snapshots while still revisiting snapshots
    where additional expiries have since landed on disk. Returns the number
    of snapshots processed.
    """
    logger.info("Processing %s", trade_date.isoformat())

    entries = discover_trade_date(trade_date)
    if not entries:
        logger.warning("No parquet files found for %s", trade_date.isoformat())
        return 0

    combined = load_trade_date(entries)
    if combined.empty:
        logger.warning("All files empty for %s", trade_date.isoformat())
        return 0

    ensure_partitions(conn, trade_date.isoformat())

    timestamps = sorted(combined["_ts"].unique())

    if diag_counts_by_qt:
        # Count current (expiry, session) groups per snapshot in the parquet.
        # Skip a snapshot only when the diagnostics row count for that
        # quote_time already matches — otherwise new expiries have landed
        # since the last run and we need to reprocess (upsert overwrites).
        per_ts_groups = (combined.groupby("_ts")[["_expiry", "_session"]]
                                 .apply(lambda g: g.drop_duplicates().shape[0])
                                 .to_dict())
        before = len(timestamps)
        kept = []
        for ts in timestamps:
            qt = pd.Timestamp(ts).time()
            n_disk = per_ts_groups.get(ts, 0)
            n_db = diag_counts_by_qt.get(qt, 0)
            if n_db >= n_disk and n_db > 0:
                continue
            kept.append(ts)
        timestamps = kept
        logger.info("  Skipping %d complete snapshots already in DB; "
                    "%d snapshots to (re)process",
                    before - len(timestamps), len(timestamps))
        if not timestamps:
            logger.info("  Nothing new to process.")
            return 0

    logger.info("  %d snapshots, %d expiry/session combos",
                len(timestamps), combined.groupby(["_expiry", "_session"]).ngroups)

    total_surface = total_atm = 0

    for ts in timestamps:
        snap_df = combined[combined["_ts"] == ts]
        snap_ts = pd.Timestamp(ts)

        try:
            surface_rows, atm_rows, diag_rows = process_snapshot(
                snap_df, snap_ts, trade_date,
                min_expiry_dte=min_expiry_dte,
                target_dtes=target_dtes,
            )
        except Exception as exc:
            logger.error("Snapshot %s failed unexpectedly: %s", ts, exc)
            continue

        if not atm_only:
            upsert_surface(conn, surface_rows)
            if write_diagnostics:
                upsert_diagnostics(conn, diag_rows)
        upsert_atm(conn, atm_rows)

        total_surface += len(surface_rows)
        total_atm     += len(atm_rows)

    logger.info("  Done: %d surface rows, %d ATM rows", total_surface, total_atm)
    return len(timestamps)


# ---------------------------------------------------------------------------
# Batch mode
# ---------------------------------------------------------------------------

def batch_run(start: date, end: date) -> None:
    """Process all trade dates in [start, end] inclusive."""
    with get_connection() as conn:
        d = start
        while d <= end:
            try:
                process_date(d, conn)
            except Exception as exc:
                logger.error("Date %s failed: %s", d.isoformat(), exc)
            d += timedelta(days=1)


# ---------------------------------------------------------------------------
# Incremental mode
# ---------------------------------------------------------------------------

def _latest_processed_date(conn: psycopg2.extensions.connection) -> date | None:
    """Return the most recent trade_date in spx_surface_diagnostics, or None."""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(trade_date) FROM spx_surface_diagnostics")
        row = cur.fetchone()
    return row[0] if row and row[0] else None


def incremental_run() -> None:
    """
    Find the latest processed date, then process every date from the day after
    through today.
    """
    with get_connection() as conn:
        latest = _latest_processed_date(conn)

    if latest is None:
        logger.warning(
            "No processed dates found in diagnostics table. "
            "Run in batch mode with an explicit --start date."
        )
        return

    start = latest + timedelta(days=1)
    end   = date.today()

    if start > end:
        logger.info("Already up to date (latest: %s)", latest.isoformat())
        return

    logger.info("Incremental run: %s → %s", start.isoformat(), end.isoformat())
    batch_run(start, end)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _prompt_date(prompt: str) -> date:
    while True:
        raw = input(prompt).strip()
        try:
            return date.fromisoformat(raw)
        except ValueError:
            print("  Invalid date — please use YYYY-MM-DD format.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SPX options surface interpolation pipeline"
    )
    sub = parser.add_subparsers(dest="command")

    # init-db
    sub.add_parser("init-db", help="Create tables and functions in PostgreSQL")

    # batch
    batch_p = sub.add_parser("batch", help="Process a date range")
    batch_p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    batch_p.add_argument("--end",   required=True, help="End date YYYY-MM-DD")

    # incremental
    sub.add_parser("incremental", help="Process dates since last run")

    args = parser.parse_args()

    if not args.command:
        # Interactive mode: prompt for dates and run batch
        print("SPX surface pipeline — batch mode")
        start = _prompt_date("  Start date (YYYY-MM-DD): ")
        end   = _prompt_date("  End date   (YYYY-MM-DD): ")
        if start > end:
            logger.error("Start date must be ≤ end date")
            sys.exit(1)
        batch_run(start, end)
        return

    if args.command == "init-db":
        init_db()

    elif args.command == "batch":
        start = date.fromisoformat(args.start)
        end   = date.fromisoformat(args.end)
        if start > end:
            logger.error("--start must be ≤ --end")
            sys.exit(1)
        batch_run(start, end)

    elif args.command == "incremental":
        incremental_run()


if __name__ == "__main__":
    main()
