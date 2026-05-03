#!/usr/bin/env python3
"""Report workflow stats to the Flight Finder API for monitoring."""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

API_URL = os.environ.get("STATS_API_URL", "https://flight-finder.co.uk/api/workflow-stats")
API_KEY = os.environ.get("STATS_API_KEY", "")


def post_stats(data: dict) -> bool:
    """Post stats to the workflow-stats API endpoint."""
    if not API_KEY:
        logger.warning("STATS_API_KEY not set, skipping stats report")
        return False
    try:
        resp = requests.post(
            f"{API_URL}?key={API_KEY}",
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.ok:
            logger.info(f"Stats reported: {data.get('step')} {data.get('status')}")
            return True
        else:
            logger.warning(f"Stats report failed: POST {API_URL}?key=*** -> {resp.status_code} {resp.text}")
            return False
    except Exception as e:
        logger.warning(f"Stats report error: {e}")
        return False


def report_scrape(airport: str, destination: str, stats_json: str):
    """Report scrape step stats from RefreshStats JSON."""
    stats = json.loads(stats_json)
    post_stats({
        "airport": airport,
        "destination": destination,
        "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "step": "scrape",
        "status": "success" if stats.get("failed", 0) == 0 else "partial",
        "started_at": stats.get("started_at", ""),
        "finished_at": stats.get("finished_at", ""),
        "duration_secs": stats.get("duration_secs", 0),
        "searches_total": stats.get("total", 0),
        "searches_completed": stats.get("completed", 0),
        "searches_failed": stats.get("failed", 0),
        "searches_no_results": stats.get("no_results", 0),
        "flights_found": stats.get("flights_found", 0),
        "flights_filtered": stats.get("flights_filtered", 0),
        "rate_limits": stats.get("rate_limits", 0),
        "scrape_time_secs": stats.get("scrape_time", 0),
        "rate_limit_wait_secs": stats.get("rate_limit_wait_time", 0),
        "unchanged": stats.get("unchanged", 0),
        "extra": json.dumps({
            "flights_skipped_no_time": stats.get("flights_skipped_no_time", 0),
            "flights_skipped_zero_price": stats.get("flights_skipped_zero_price", 0),
            "destinations_searched": stats.get("destinations_searched", 0),
            "dates_searched": stats.get("dates_searched", 0),
            "avg_per_search": stats.get("avg_per_search", 0),
            "avg_scrape_time": stats.get("avg_scrape_time", 0),
        }),
    })


def report_export(airport: str, destination: str, exported_searches: int,
                  exported_flights: int, unchanged: int, size_kb: float, duration_secs: float):
    """Report export step stats."""
    post_stats({
        "airport": airport,
        "destination": destination,
        "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "step": "export",
        "status": "success",
        "duration_secs": duration_secs,
        "export_searches": exported_searches,
        "export_flights": exported_flights,
        "export_size_kb": size_kb,
        "unchanged": unchanged,
    })


def report_import(airport: str, destination: str, attempts: int, success: bool,
                  duration_secs: float, error_message: str = ""):
    """Report D1 import step stats."""
    post_stats({
        "airport": airport,
        "destination": destination,
        "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "step": "import",
        "status": "success" if success else "error",
        "duration_secs": duration_secs,
        "import_attempts": attempts,
        "import_success": 1 if success else 0,
        "error_message": error_message,
    })


def report_error(airport: str, destination: str, step: str, error_message: str):
    """Report an error in any step."""
    post_stats({
        "airport": airport,
        "destination": destination,
        "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "step": step,
        "status": "error",
        "error_message": error_message,
    })


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Simple CLI: python report_stats.py <step> <json_data>
    if len(sys.argv) >= 3:
        step = sys.argv[1]
        data = json.loads(sys.argv[2])
        data["step"] = step
        data["run_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        data["run_id"] = os.environ.get("GITHUB_RUN_ID", "")
        post_stats(data)
