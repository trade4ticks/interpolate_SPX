"""
process_intraday.py — Re-run the surface pipeline for today's trade_date.

Designed to run via cron every few minutes after step-2 (clean_SPX) finishes
its intraday processing. Each run reloads all of today's parquet files and
re-processes every snapshot; existing rows are overwritten via upsert, so the
job is idempotent and safe to call repeatedly.

Cron example (run a couple minutes after clean_SPX intraday):
  3-59/5 9-16 * * 1-5  /path/to/venv/Scripts/python.exe \\
      C:/Personal/Data/interpolate_SPX/scripts/process_intraday.py

Exits immediately on weekends or outside market hours.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, time, timedelta
from pathlib import Path

# Make the project root importable when invoked directly by cron
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytz

from pipeline.run import process_date
from pipeline.store import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_ET = pytz.timezone("US/Eastern")

# Market hours window (ET). Start a few minutes after the open so the first
# snapshot from step-2 is available; allow a tail past the close to catch the
# final bar.
_OPEN_TIME  = time(9, 35)
_CLOSE_TIME = time(17, 0)


def _is_trading_day_et(now_et: datetime) -> bool:
    """Cheap weekday check. Holidays still run but exit harmlessly when
    discover_trade_date() finds no files."""
    return now_et.weekday() < 5


def main() -> None:
    now_et = datetime.now(_ET)

    if not _is_trading_day_et(now_et):
        log.info("Not a weekday — nothing to do.")
        return

    t = now_et.time()
    if t < _OPEN_TIME or t > _CLOSE_TIME:
        log.info("Outside market hours (%s ET) — nothing to do.",
                 now_et.strftime("%H:%M:%S"))
        return

    today = now_et.date()
    log.info("Intraday run for %s", today.isoformat())

    with get_connection() as conn:
        # Pull the SET of quote_times already in the DB for today and skip
        # exactly those. Using a set (rather than MAX) lets late-arriving
        # snapshots from steps 1/2 be filled in on a later run instead of
        # being permanently skipped because something above them landed
        # first.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT quote_time FROM spx_surface_diagnostics "
                "WHERE trade_date = %s",
                (today,),
            )
            done_qts = {r[0] for r in cur.fetchall()}
        log.info("Already in DB: %d snapshots for %s",
                 len(done_qts), today.isoformat())

        try:
            process_date(today, conn, skip_quote_times=done_qts)
        except Exception as exc:
            log.error("process_date(%s) failed: %s", today.isoformat(), exc)
            sys.exit(1)


if __name__ == "__main__":
    main()
