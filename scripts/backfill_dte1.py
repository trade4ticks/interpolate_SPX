"""
backfill_dte1.py — Recompute the dte=1 bucket only, for a date range.

After the sample_surface fix that emits dte=1 on Tue/Thu in 2021 via the
nearest-PM-expiry fallback (DirectSmile on usable[0] when target_T <
usable[0].T), run this to backfill the now-missing dte=1 rows. On days
with a same-day expiry (Mon/Wed/Fri pre-May-2022, all weekdays after),
dte=1 brackets normally between the same-day fit and the next PM fit.

Speed: by limiting target_dtes to [1] and capping --max-expiry-dte, only
the 1-2 nearest PM expiries get fit per snapshot, instead of all ~30.
Typical speedup is 4-5x vs a full reprocess.

Existing spx_surface and spx_atm rows for dte=1 are overwritten via
upsert; rows for other DTEs are left untouched. Diagnostics rows are
NOT written (a partial fit set would corrupt the existing diagnostics
row counts), so the intraday cron's "complete snapshot" check still works.

Usage:
    # Tue/Thu only over the no-same-day-expiry window (default weekdays)
    python scripts/backfill_dte1.py --start 2021-01-01 --end 2022-05-11

    # All weekdays (Mon/Wed/Fri dte=1 will upsert identically — wasted work)
    python scripts/backfill_dte1.py --start 2021-01-01 --end 2022-05-11 \\
        --weekdays all
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# Make the project root importable when invoked directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.run import process_date
from pipeline.store import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


_WEEKDAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4}


def _parse_weekdays(spec: str) -> set[int]:
    spec = spec.strip().lower()
    if spec == "all":
        return set(range(5))
    out: set[int] = set()
    for token in spec.split(","):
        token = token.strip().lower()
        if token not in _WEEKDAY_MAP:
            raise ValueError(f"unknown weekday: {token!r}")
        out.add(_WEEKDAY_MAP[token])
    if not out:
        raise ValueError("--weekdays produced an empty set")
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", required=True, help="Start date YYYY-MM-DD (inclusive)")
    p.add_argument("--end",   required=True, help="End date YYYY-MM-DD (inclusive)")
    p.add_argument(
        "--weekdays", default="tue,thu",
        help="Comma-separated weekdays to process: mon,tue,wed,thu,fri, "
             "or 'all'. Default: tue,thu (only days where dte=1 changed).",
    )
    p.add_argument(
        "--max-expiry-dte", type=int, default=14,
        help="Skip expiries with calendar DTE greater than this (default: 14). "
             "Must be large enough to keep >= 2 PM expiries on every day, "
             "including weeks with holidays.",
    )
    args = p.parse_args()

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    if start > end:
        log.error("--start must be <= --end")
        sys.exit(1)

    try:
        wanted_dows = _parse_weekdays(args.weekdays)
    except ValueError as exc:
        log.error(str(exc))
        sys.exit(1)

    log.info("Backfilling dte=1 for %s → %s; weekdays=%s; max_expiry_dte=%d",
             start.isoformat(), end.isoformat(),
             sorted(wanted_dows), args.max_expiry_dte)

    processed = 0
    with get_connection() as conn:
        d = start
        while d <= end:
            if d.weekday() in wanted_dows:
                try:
                    process_date(
                        d, conn,
                        target_dtes=[1],
                        max_expiry_dte=args.max_expiry_dte,
                        write_diagnostics=False,
                    )
                    processed += 1
                except Exception as exc:
                    log.error("Date %s failed: %s", d.isoformat(), exc)
            d += timedelta(days=1)

    log.info("Done. Processed %d trade_date(s).", processed)


if __name__ == "__main__":
    main()
