"""Core refresh logic — scrapes Google Flights to local SQLite."""
from __future__ import annotations

import calendar
import json
import logging
import random
import re
import time
from datetime import date, datetime
from typing import Callable, Optional

from cache_db import FlightCache, _parse_time_to_minutes
from google_flights import search_flights
from rate_limiter import RateLimiter, AbortError
from config import CHROME_VERSIONS, CONSENT_COOKIES

logger = logging.getLogger(__name__)


def _parse_price(price_str: str) -> float:
    if not price_str:
        return 0.0
    nums = re.sub(r"[^\d.]", "", price_str.replace(",", ""))
    try:
        return float(nums)
    except ValueError:
        return 0.0



def _get_month_dates(month_str: str) -> list:
    year, month = int(month_str[:4]), int(month_str[5:7])
    _, num_days = calendar.monthrange(year, month)
    today = date.today()
    return [
        date(year, month, day).isoformat()
        for day in range(1, num_days + 1)
        if date(year, month, day) > today
    ]


def build_search_queue(origin: str, destinations: dict, month: str) -> list:
    """Build a search queue — refreshes everything, no staleness checks.

    Prices change unpredictably at any distance, so all data is
    refreshed on every run. Destinations are shuffled per date
    to look more human to Google.
    """
    dates = _get_month_dates(month)
    if not dates:
        return []

    queue = []
    for flight_date in dates:
        dest_list = list(destinations.items())
        random.shuffle(dest_list)
        for dest_code, dest_name in dest_list:
            for direction in ("outbound", "return"):
                o, d = (origin, dest_code) if direction == "outbound" else (dest_code, origin)
                queue.append((o, d, flight_date, direction))

    return queue


class RefreshStats:
    """Tracks detailed timing and counts for the refresh run."""

    def __init__(self):
        self.start_time = time.time()
        self.completed = 0
        self.failed = 0
        self.skipped = 0
        self.total = 0
        self.flights_found = 0
        self.flights_skipped_no_time = 0
        self.flights_filtered = 0
        self.unchanged = 0
        self.no_results = 0
        self.rate_limits = 0
        self.scrape_time = 0.0
        self.rate_limit_wait_time = 0.0
        self.rate_limiter_report = ""
        self.destinations_searched = set()
        self.dates_searched = set()

    def report(self) -> str:
        elapsed = time.time() - self.start_time
        lines = [
            "",
            "=" * 60,
            "REFRESH STATUS REPORT",
            "=" * 60,
            f"Total time:          {elapsed/60:.1f} minutes",
            f"Searches completed:  {self.completed} / {self.total}",
            f"  - With flights:    {self.completed - self.no_results}",
            f"  - No results:      {self.no_results}",
            f"Failed:              {self.failed}",
            f"  - Rate limits:     {self.rate_limits}",
            f"Skipped (fresh):     {self.skipped}",
            f"Flights found:       {self.flights_found}",
            f"Flights skipped:     {self.flights_skipped_no_time} (no time data)",
            f"Flights filtered:    {self.flights_filtered} (wrong time/stops for day trips)",
            f"Unchanged searches:  {self.unchanged} (data identical, skip D1 write)",
            f"Destinations:        {len(self.destinations_searched)}",
            f"Dates covered:       {len(self.dates_searched)}",
            "",
            "TIME BREAKDOWN:",
            f"  Scraping:          {self.scrape_time:.1f}s ({self.scrape_time/max(elapsed,1)*100:.0f}%)",
            f"  Rate limit waits:  {self.rate_limit_wait_time:.1f}s ({self.rate_limit_wait_time/max(elapsed,1)*100:.0f}%)",
            f"  Other overhead:    {max(0, elapsed - self.scrape_time - self.rate_limit_wait_time):.1f}s",
            f"  (D1 sync via wrangler bulk import — separate step)",
            "",
            f"Avg per search:      {elapsed/max(self.completed,1):.2f}s",
            f"Avg scrape time:     {self.scrape_time/max(self.completed,1):.2f}s",
            "",
            "RATE LIMITER:",
            self.rate_limiter_report or "  (no data)",
            "=" * 60,
        ]
        return "\n".join(lines)


def run_refresh(
    cache: FlightCache,
    origin: str,
    destinations: dict,
    month: str,
    progress_callback: Optional[Callable] = None,
) -> RefreshStats:
    """Run a full refresh cycle — scrapes all flights to local SQLite."""
    import os

    for dest_code, dest_name in destinations.items():
        cache.upsert_route(origin, dest_code, dest_name)

    queue = build_search_queue(origin, destinations, month)
    stats = RefreshStats()
    stats.total = len(queue)

    if stats.total == 0:
        logger.info("No dates to search")
        return stats

    logger.info(f"Refresh queue: {stats.total} searches")

    rate_limiter = RateLimiter()
    chrome_version = random.choice(CHROME_VERSIONS)
    cookie_idx = 0

    is_ci = os.environ.get("CI") or os.environ.get("GITHUB_ACTIONS")
    last_log = [0]

    for i, (o, d, flight_date, direction) in enumerate(queue):
        if progress_callback:
            progress_callback(i + 1, stats.total, o, d, flight_date, direction,
                              stats.completed, stats.failed, stats.flights_found)

        # Log progress in CI
        if is_ci:
            now = time.time()
            if i % 10 == 0 or (now - last_log[0]) > 30:
                elapsed = now - stats.start_time
                print(f"[{i+1}/{stats.total}] {o}->{d} {flight_date} {direction} | "
                      f"done={stats.completed} fail={stats.failed} flights={stats.flights_found} | "
                      f"{elapsed/60:.1f}min elapsed", flush=True)
                last_log[0] = now

        # Rate limit wait
        try:
            wait_start = time.time()
            rate_limiter.wait()
            stats.rate_limit_wait_time += time.time() - wait_start
        except AbortError:
            logger.error("Refresh aborted due to too many errors")
            break

        cookie_str = CONSENT_COOKIES[cookie_idx % len(CONSENT_COOKIES)]
        now_str = datetime.utcnow().isoformat()

        # Scrape
        scrape_start = time.time()
        try:
            result = search_flights(
                from_airport=o, to_airport=d, date=flight_date,
                max_stops=0,  # direct flights only
                cookie_str=cookie_str, chrome_version=chrome_version,
            )

            flights = []
            skipped_no_time = 0
            skipped_filtered = 0
            if result and result.flights:
                for f in result.flights:
                    dep_time = f.departure or ""
                    arr_time = f.arrival or ""

                    # Skip flights with no departure or arrival time
                    if not dep_time or not arr_time:
                        skipped_no_time += 1
                        continue

                    dep_mins = _parse_time_to_minutes(dep_time)
                    arr_mins = _parse_time_to_minutes(arr_time)

                    # Skip flights where time couldn't be parsed
                    if dep_mins < 0 or arr_mins < 0:
                        skipped_no_time += 1
                        continue

                    # Skip non-direct flights (shouldn't happen with max_stops=0 but safety check)
                    stops = f.stops if isinstance(f.stops, int) else 0
                    if stops > 0:
                        skipped_filtered += 1
                        continue

                    # Skip next-day arrivals on outbound (landing next day is useless)
                    # But allow on return flights (arriving home after midnight is fine)
                    arrival_ahead = getattr(f, "arrival_time_ahead", "") or ""
                    if arrival_ahead and arrival_ahead != "0" and direction == "outbound":
                        skipped_filtered += 1
                        continue

                    # For outbound: only keep flights departing before noon
                    if direction == "outbound" and dep_mins >= 720:
                        skipped_filtered += 1
                        continue

                    # For return: only keep flights departing afternoon or later
                    if direction == "return" and dep_mins < 720:
                        skipped_filtered += 1
                        continue

                    flights.append({
                        "airline": f.name or "",
                        "departure": dep_time,
                        "arrival": arr_time,
                        "depart_minutes": dep_mins,
                        "arrive_minutes": arr_mins,
                        "price": _parse_price(f.price),
                        "currency": "GBP",
                        "stops": stops,
                        "arrival_ahead": arrival_ahead,
                    })

            stats.flights_skipped_no_time += skipped_no_time
            stats.flights_filtered += skipped_filtered

            # Deduplicate flights (Google sometimes returns same flight twice)
            seen_flights = set()
            unique_flights = []
            for fl in flights:
                key = (fl["airline"], fl["departure"], fl["arrival"], fl["price"])
                if key not in seen_flights:
                    seen_flights.add(key)
                    unique_flights.append(fl)
            flights = unique_flights

            stats.scrape_time += time.time() - scrape_start
            search_status = "success" if flights else "no_results"
            stats.flights_found += len(flights)
            if not flights:
                stats.no_results += 1

            # Save to local SQLite
            changed = cache.record_search(o, d, flight_date, direction, status=search_status, flights=flights)
            if not changed:
                stats.unchanged += 1

            rate_limiter.record_success()
            stats.completed += 1
            stats.destinations_searched.add(d if direction == "outbound" else o)
            stats.dates_searched.add(flight_date)

        except (AssertionError, Exception) as e:
            stats.scrape_time += time.time() - scrape_start
            is_rate_limit = "429" in str(e) or "503" in str(e)
            if is_rate_limit:
                stats.rate_limits += 1
            logger.warning(f"Failed {o}->{d} {flight_date} {direction}: {e}")
            rate_limiter.record_error(is_rate_limit=is_rate_limit)
            cookie_idx += 1

            # Retry once after backoff
            logger.info(f"Retrying {o}->{d} {flight_date} {direction}...")
            try:
                rate_limiter.wait()
                retry_start = time.time()
                retry_cookie = CONSENT_COOKIES[cookie_idx % len(CONSENT_COOKIES)]
                result = search_flights(
                    from_airport=o, to_airport=d, date=flight_date,
                    max_stops=0, cookie_str=retry_cookie, chrome_version=chrome_version,
                )
                flights = []
                skipped_no_time = 0
                skipped_filtered = 0
                if result and result.flights:
                    for f in result.flights:
                        dep_time = f.departure or ""
                        arr_time = f.arrival or ""
                        if not dep_time or not arr_time:
                            skipped_no_time += 1
                            continue
                        dep_mins = _parse_time_to_minutes(dep_time)
                        arr_mins = _parse_time_to_minutes(arr_time)
                        if dep_mins < 0 or arr_mins < 0:
                            skipped_no_time += 1
                            continue
                        stops = f.stops if isinstance(f.stops, int) else 0
                        if stops > 0:
                            skipped_filtered += 1
                            continue
                        arrival_ahead = getattr(f, "arrival_time_ahead", "") or ""
                        if arrival_ahead and arrival_ahead != "0" and direction == "outbound":
                            skipped_filtered += 1
                            continue
                        if direction == "outbound" and dep_mins >= 720:
                            skipped_filtered += 1
                            continue
                        if direction == "return" and dep_mins < 720:
                            skipped_filtered += 1
                            continue
                        flights.append({
                            "airline": f.name or "", "departure": dep_time, "arrival": arr_time,
                            "depart_minutes": dep_mins, "arrive_minutes": arr_mins,
                            "price": _parse_price(f.price), "currency": "GBP",
                            "stops": stops, "arrival_ahead": arrival_ahead,
                        })
                seen_flights = set()
                unique_flights = []
                for fl in flights:
                    key = (fl["airline"], fl["departure"], fl["arrival"], fl["price"])
                    if key not in seen_flights:
                        seen_flights.add(key)
                        unique_flights.append(fl)
                flights = unique_flights

                stats.scrape_time += time.time() - retry_start
                search_status = "success" if flights else "no_results"
                stats.flights_found += len(flights)
                cache.record_search(o, d, flight_date, direction, status=search_status, flights=flights)
                rate_limiter.record_success()
                stats.completed += 1
                logger.info(f"Retry succeeded: {o}->{d} {flight_date} {direction} ({len(flights)} flights)")
            except Exception as retry_e:
                logger.warning(f"Retry also failed: {o}->{d} {flight_date} {direction}: {retry_e}")
                cache.record_search(o, d, flight_date, direction, status="error", error_msg=str(e))
                stats.failed += 1

    cache.cleanup_expired()

    stats.rate_limiter_report = rate_limiter.report()
    return stats
