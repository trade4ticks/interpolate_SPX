"""
backfill_long_dtes.py — Recompute long-DTE surface rows for a date range.

Targeted backfill that only fits the expiries needed to bracket a small set
of long-dated DTE targets (default: 180, 270, 360). Much faster than a full
re-run because front-month expiries — which dominate fitting time but are
irrelevant for long-dated interpolation — are skipped entirely.

Existing spx_surface and spx_atm rows for the targeted DTEs are overwritten
via upsert; rows for other DTEs are left untouched. Diagnostics rows are
NOT written (a partial fit set would corrupt the existing diagnostics row
counts), so the intraday cron's "complete snapshot" check still works.

Usage:
    python scripts/backfill_long_dtes.py --start 2024-01-01 --end 2024-12-31
    python scripts/backfill_long_dtes.py --start 2024-06-01 --end 2024-06-30 \\
        --dtes 270,360 --min-dte 200
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


def _prompt_date(prompt: str) -> date:
    while True:
        raw = input(prompt).strip()
        try:
            return date.fromisoformat(raw)
        except ValueError:
            print("  Invalid date — please use YYYY-MM-DD format.")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", help="Start date YYYY-MM-DD (prompted if omitted)")
    p.add_argument("--end",   help="End date YYYY-MM-DD (prompted if omitted)")
    p.add_argument(
        "--dtes", default="180,270,360",
        help="Comma-separated target DTEs to recompute (default: 180,270,360)",
    )
    p.add_argument(
        "--min-dte", type=int, default=150,
        help="Only fit expiries with calendar DTE >= this value (default: 150). "
             "Must leave at least one expiry below the lowest target DTE so "
             "that target can be bracketed.",
    )
    args = p.parse_args()

    if not args.start or not args.end:
        print("Long-DTE backfill — date range")
        start = _prompt_date("  Start date (YYYY-MM-DD): ") if not args.start else date.fromisoformat(args.start)
        end   = _prompt_date("  End date   (YYYY-MM-DD): ") if not args.end   else date.fromisoformat(args.end)
    else:
        start = date.fromisoformat(args.start)
        end   = date.fromisoformat(args.end)
    if start > end:
        log.error("--start must be <= --end")
        sys.exit(1)

    target_dtes = [int(x) for x in args.dtes.split(",") if x.strip()]
    if not target_dtes:
        log.error("--dtes must contain at least one value")
        sys.exit(1)

    if args.min_dte >= min(target_dtes):
        log.error("--min-dte (%d) must be < min target DTE (%d) so the "
                  "lowest target can be bracketed from below",
                  args.min_dte, min(target_dtes))
        sys.exit(1)

    log.info("Backfilling DTEs %s for %s → %s (min expiry DTE = %d)",
             target_dtes, start.isoformat(), end.isoformat(), args.min_dte)

    with get_connection() as conn:
        d = start
        while d <= end:
            try:
                process_date(
                    d, conn,
                    min_expiry_dte=args.min_dte,
                    target_dtes=target_dtes,
                    write_diagnostics=False,
                )
            except Exception as exc:
                log.error("Date %s failed: %s", d.isoformat(), exc)
            d += timedelta(days=1)


if __name__ == "__main__":
    main()
