#!/usr/bin/env python3
"""
Flight Cache Refresher — populates flight data from Google Flights.
Syncs to Cloudflare D1 in real-time during rate limit pauses.
"""
from __future__ import annotations

import argparse
import atexit
import logging
import os
import signal
import sys
import time

from config import CACHE_DIR, LOCK_PATH, LOG_PATH, AIRPORT
from cache_db import FlightCache
from destinations import get_destinations, get_airport_name
from refresh_worker import run_refresh

logger = logging.getLogger(__name__)


def _acquire_lock() -> bool:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if LOCK_PATH.exists():
        try:
            pid = int(LOCK_PATH.read_text().strip())
            os.kill(pid, 0)
            logger.error(f"Another refresh is running (PID {pid})")
            return False
        except (ProcessLookupError, ValueError):
            logger.warning("Stale lock file found, taking over")
    LOCK_PATH.write_text(str(os.getpid()))
    atexit.register(lambda: LOCK_PATH.unlink(missing_ok=True))
    return True


def _setup_logging(verbose: bool):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    handlers = [logging.StreamHandler()]
    try:
        handlers.append(logging.FileHandler(str(LOG_PATH)))
    except Exception:
        pass
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )
    logging.getLogger("primp").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main() -> int:
    parser = argparse.ArgumentParser(description="Flight cache refresher")
    parser.add_argument("--month", required=True, help="Month to refresh (YYYY-MM)")
    parser.add_argument("--airport", default=AIRPORT, help=f"Origin airport (default: {AIRPORT})")
    parser.add_argument("--destinations", help="Comma-separated IATA codes (default: all)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    _setup_logging(args.verbose)

    if not _acquire_lock():
        return 1

    airport = args.airport.upper()
    if args.destinations:
        codes = [c.strip().upper() for c in args.destinations.split(",")]
        all_dests = get_destinations(airport)
        destinations = {c: all_dests.get(c, c) for c in codes}
    else:
        destinations = get_destinations(airport)

    if not destinations:
        logger.error(f"No destinations configured for {airport}")
        return 1

    cache = FlightCache()
    cache.upsert_airport(airport, get_airport_name(airport), is_origin=True)
    for code, name in destinations.items():
        cache.upsert_airport(code, name)

    is_ci = os.environ.get("CI") or os.environ.get("GITHUB_ACTIONS")

    def on_progress(current, total, o, d, flight_date, direction, completed, failed, flights_found):
        if not is_ci:
            pct = current / total * 100
            print(f"\r  [{pct:5.1f}%] {current}/{total} | {o}->{d} {flight_date} {direction} | "
                  f"done={completed} fail={failed} flights={flights_found}",
                  end="", flush=True)

    logger.info(f"Starting refresh: {airport} -> {len(destinations)} destinations, {args.month}")

    stats = run_refresh(
        cache=cache, origin=airport, destinations=destinations,
        month=args.month, progress_callback=on_progress,
    )

    if not is_ci:
        print()  # clear progress line

    # Print status report
    report = stats.report()
    print(report, flush=True)
    logger.info(report)

    db_stats = cache.get_stats()
    print(f"\nDatabase totals: {db_stats['searches']} searches, {db_stats['flights']} flights, {db_stats['routes']} routes")
    cache.close()

    # Output stats as JSON for CI — accumulate across months
    import json
    elapsed = time.time() - stats.start_time
    stats_json = {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(stats.start_time)),
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        "duration_secs": round(elapsed, 1),
        "total": stats.total,
        "completed": stats.completed,
        "failed": stats.failed,
        "no_results": stats.no_results,
        "flights_found": stats.flights_found,
        "flights_filtered": stats.flights_filtered,
        "flights_skipped_no_time": stats.flights_skipped_no_time,
        "flights_skipped_zero_price": stats.flights_skipped_zero_price,
        "rate_limits": stats.rate_limits,
        "scrape_time": round(stats.scrape_time, 1),
        "rate_limit_wait_time": round(stats.rate_limit_wait_time, 1),
        "unchanged": stats.unchanged,
        "destinations_searched": len(stats.destinations_searched),
        "dates_searched": len(stats.dates_searched),
        "avg_per_search": round(elapsed / max(stats.completed, 1), 2),
        "avg_scrape_time": round(stats.scrape_time / max(stats.completed, 1), 2),
    }

    # Accumulate with previous months' stats (workflow runs multiple months per job)
    stats_path = CACHE_DIR / "last_stats.json"
    if stats_path.exists():
        try:
            prev = json.loads(stats_path.read_text())
            for key in ["total", "completed", "failed", "no_results", "flights_found",
                         "flights_filtered", "flights_skipped_no_time", "flights_skipped_zero_price", "rate_limits", "unchanged"]:
                stats_json[key] = stats_json.get(key, 0) + prev.get(key, 0)
            stats_json["scrape_time"] = round(stats_json["scrape_time"] + prev.get("scrape_time", 0), 1)
            stats_json["rate_limit_wait_time"] = round(stats_json["rate_limit_wait_time"] + prev.get("rate_limit_wait_time", 0), 1)
            stats_json["duration_secs"] = round(stats_json["duration_secs"] + prev.get("duration_secs", 0), 1)
            stats_json["dates_searched"] = stats_json["dates_searched"] + prev.get("dates_searched", 0)
            stats_json["started_at"] = prev.get("started_at", stats_json["started_at"])
            total_completed = stats_json["completed"]
            total_elapsed = stats_json["duration_secs"]
            stats_json["avg_per_search"] = round(total_elapsed / max(total_completed, 1), 2)
            stats_json["avg_scrape_time"] = round(stats_json["scrape_time"] / max(total_completed, 1), 2)
        except Exception:
            pass

    stats_path.write_text(json.dumps(stats_json))
    print(f"\nStats written to {stats_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
