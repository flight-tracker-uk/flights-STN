#!/usr/bin/env python3
"""Cloudflare D1 sync — real-time and batch modes."""
from __future__ import annotations

import logging
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from threading import Thread

import requests

logger = logging.getLogger(__name__)

CF_API_BASE = "https://api.cloudflare.com/client/v4"
D1_WORKERS = 5  # parallel D1 API calls


class D1Client:
    """Cloudflare D1 REST API client."""

    def __init__(self):
        self.api_token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
        self.account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "")
        self.database_id = os.environ.get("CLOUDFLARE_D1_DATABASE_ID", "")
        self.url = f"{CF_API_BASE}/accounts/{self.account_id}/d1/database/{self.database_id}/query"
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }
        self._stats = {"api_calls": 0, "rows_synced": 0, "errors": 0, "time_spent": 0}

    @property
    def is_configured(self) -> bool:
        return bool(self.api_token and self.account_id and self.database_id)

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    def _run(self, sql: str, params: list = None) -> dict:
        """Execute a single SQL statement via D1 REST API."""
        start = time.time()
        body = {"sql": sql}
        if params:
            body["params"] = params
        try:
            resp = requests.post(self.url, headers=self.headers, json=body, timeout=30)
            resp.raise_for_status()
            self._stats["api_calls"] += 1
            self._stats["rows_synced"] += 1
            self._stats["time_spent"] += time.time() - start
            data = resp.json()
            if "result" in data and data["result"]:
                return data["result"][0]
            return {}
        except requests.exceptions.HTTPError as e:
            self._stats["errors"] += 1
            self._stats["time_spent"] += time.time() - start
            try:
                err_body = e.response.text[:300]
            except Exception:
                err_body = ""
            logger.warning(f"D1 error: {e} | {err_body}")
            return {}
        except Exception as e:
            self._stats["errors"] += 1
            self._stats["time_spent"] += time.time() - start
            logger.warning(f"D1 error: {e}")
            return {}

    def _query(self, sql: str, params: list = None) -> list:
        """Execute a query and return the results list."""
        result = self._run(sql, params)
        return result.get("results", [])

    def start_background_sync(self):
        """Start background thread pool for async D1 syncing."""
        self._queue = Queue()
        self._pool = ThreadPoolExecutor(max_workers=D1_WORKERS)
        self._bg_thread = Thread(target=self._bg_worker, daemon=True)
        self._bg_thread.start()
        logger.info(f"D1 background sync started ({D1_WORKERS} workers)")

    def _bg_worker(self):
        """Process sync tasks from the queue."""
        while True:
            task = self._queue.get()
            if task is None:
                self._queue.task_done()
                break
            try:
                self._do_sync_search(*task)
            except Exception as e:
                logger.debug(f"Background sync error: {e}")
            self._queue.task_done()

    def wait_for_sync(self):
        """Wait for all queued syncs to complete."""
        if hasattr(self, "_queue"):
            self._queue.join()

    def stop_background_sync(self):
        """Stop the background sync thread."""
        if hasattr(self, "_queue"):
            self._queue.put(None)
            self._bg_thread.join(timeout=30)

    def sync_search(self, origin: str, dest: str, flight_date: str, direction: str,
                    searched_at: str, status: str, error_msg: str, flights: list):
        """Queue a search sync (runs in background if started, otherwise synchronous)."""
        if not self.is_configured:
            return
        args = (origin, dest, flight_date, direction, searched_at, status, error_msg, flights)
        if hasattr(self, "_queue"):
            self._queue.put(args)
        else:
            self._do_sync_search(*args)

    def _do_sync_search(self, origin, dest, flight_date, direction, searched_at, status, error_msg, flights):
        """Actually sync a search and its flights to D1."""
        # Step 1: Upsert search
        self._run(
            "INSERT INTO searches(origin, destination, flight_date, direction, searched_at, status, error_message, flight_count) "
            "VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(origin, destination, flight_date, direction) DO UPDATE SET "
            "searched_at=excluded.searched_at, status=excluded.status, error_message=excluded.error_message, "
            "flight_count=excluded.flight_count",
            [origin, dest, flight_date, direction, searched_at, status, error_msg, len(flights)],
        )

        # Step 2: Get the search ID
        rows = self._query(
            "SELECT id FROM searches WHERE origin=? AND destination=? AND flight_date=? AND direction=?",
            [origin, dest, flight_date, direction],
        )
        if not rows:
            return
        search_id = str(rows[0]["id"])

        # Step 3: Delete old flights
        self._run("DELETE FROM flights WHERE search_id=?", [search_id])

        # Step 4: Insert flights with multi-value INSERT (9 per call, 99 params)
        if not flights:
            return

        CHUNK = 9
        for i in range(0, len(flights), CHUNK):
            chunk = flights[i:i + CHUNK]
            placeholders = ",".join(["(?,?,?,?,?,?,?,?,?,?,?)"] * len(chunk))
            params = []
            for f in chunk:
                params.extend([
                    search_id, f.get("airline", ""), f.get("departure", ""), f.get("arrival", ""),
                    str(f.get("depart_minutes", 0)), str(f.get("arrive_minutes", 0)),
                    str(f.get("price", 0)), f.get("currency", "GBP"), str(f.get("stops", 0)),
                    f.get("arrival_ahead", ""), searched_at,
                ])
            self._run(
                f"INSERT INTO flights(search_id, airline, departure_time, arrival_time, "
                f"depart_minutes, arrive_minutes, price, currency, stops, arrival_ahead, created_at) "
                f"VALUES {placeholders}",
                params,
            )

    def sync_airports_and_routes(self, db_path: str):
        """Sync airports and routes from local DB."""
        if not self.is_configured:
            return

        local = sqlite3.connect(db_path)
        local.row_factory = sqlite3.Row

        for a in local.execute("SELECT * FROM airports").fetchall():
            self._run(
                "INSERT INTO airports(iata_code, name, country, is_origin) VALUES(?,?,?,?) "
                "ON CONFLICT(iata_code) DO UPDATE SET name=excluded.name, country=excluded.country, "
                "is_origin=MAX(is_origin, excluded.is_origin)",
                [a["iata_code"], a["name"], a["country"], a["is_origin"]],
            )

        for r in local.execute("SELECT * FROM routes").fetchall():
            self._run(
                "INSERT INTO routes(origin, destination, dest_name, is_active) VALUES(?,?,?,?) "
                "ON CONFLICT(origin, destination) DO UPDATE SET dest_name=excluded.dest_name, is_active=excluded.is_active",
                [r["origin"], r["destination"], r["dest_name"], r["is_active"]],
            )

        local.close()
        logger.info(f"Synced airports and routes to D1")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    db_path = os.path.expanduser("~/.flightcache/flights.db")
    if not Path(db_path).exists():
        logger.error(f"Database not found: {db_path}")
        return 1

    client = D1Client()
    if not client.is_configured:
        logger.error("Cloudflare credentials not set")
        return 1

    client.sync_airports_and_routes(db_path)

    local = sqlite3.connect(db_path)
    local.row_factory = sqlite3.Row
    searches = local.execute("SELECT * FROM searches").fetchall()
    logger.info(f"Syncing {len(searches)} searches...")

    for s in searches:
        flight_rows = local.execute("SELECT * FROM flights WHERE search_id=?", (s["id"],)).fetchall()
        flights = [{
            "airline": f["airline"], "departure": f["departure_time"], "arrival": f["arrival_time"],
            "depart_minutes": f["depart_minutes"], "arrive_minutes": f["arrive_minutes"],
            "price": f["price"], "currency": f["currency"], "stops": f["stops"],
            "arrival_ahead": f["arrival_ahead"],
        } for f in flight_rows]
        client.sync_search(s["origin"], s["destination"], s["flight_date"], s["direction"],
                           s["searched_at"], s["status"], s["error_message"], flights)

    local.close()
    stats = client.stats
    logger.info(f"Done: {stats['api_calls']} calls, {stats['rows_synced']} rows, {stats['errors']} errors, {stats['time_spent']:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
