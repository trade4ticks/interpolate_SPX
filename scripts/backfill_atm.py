"""
Backfill spx_atm for already-processed trade_dates.

Re-reads the parquet files for each distinct trade_date present in
spx_surface_diagnostics, re-runs fit + sample, and UPSERTs into spx_atm only.
spx_surface and spx_surface_diagnostics are NOT touched.

Usage:
    python -m scripts.backfill_atm                    # all processed dates
    python -m scripts.backfill_atm --start 2024-01-01 # from this date forward
    python -m scripts.backfill_atm --start 2024-01-01 --end 2024-06-30
"""
from __future__ import annotations

import argparse
import logging
from datetime import date

from pipeline.run import process_date
from pipeline.store import get_connection, init_db

logger = logging.getLogger("backfill_atm")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _processed_dates(conn, start: date | None, end: date | None) -> list[date]:
    sql = "SELECT DISTINCT trade_date FROM spx_surface_diagnostics"
    conds, params = [], []
    if start:
        conds.append("trade_date >= %s")
        params.append(start)
    if end:
        conds.append("trade_date <= %s")
        params.append(end)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY trade_date"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [r[0] for r in cur.fetchall()]


def main() -> None:
    p = argparse.ArgumentParser(description="Backfill spx_atm for processed dates")
    p.add_argument("--start", help="Start date YYYY-MM-DD (inclusive)")
    p.add_argument("--end",   help="End date YYYY-MM-DD (inclusive)")
    args = p.parse_args()

    start = date.fromisoformat(args.start) if args.start else None
    end   = date.fromisoformat(args.end)   if args.end   else None

    # Ensure schema is current (adds underlying_price column if missing)
    init_db()

    with get_connection() as conn:
        dates = _processed_dates(conn, start, end)
        logger.info("Backfilling %d trade_date(s)", len(dates))

        for d in dates:
            try:
                process_date(d, conn, atm_only=True)
            except Exception as exc:
                logger.error("Date %s failed: %s", d.isoformat(), exc)


if __name__ == "__main__":
    main()
