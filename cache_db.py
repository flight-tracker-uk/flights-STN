"""SQLite database layer for flight cache."""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from config import CACHE_DIR, DB_PATH

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS airports (
    iata_code   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    country     TEXT DEFAULT 'UK',
    is_origin   INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS routes (
    id          INTEGER PRIMARY KEY,
    origin      TEXT NOT NULL,
    destination TEXT NOT NULL,
    dest_name   TEXT DEFAULT '',
    is_active   INTEGER DEFAULT 1,
    UNIQUE(origin, destination)
);

CREATE TABLE IF NOT EXISTS searches (
    id              INTEGER PRIMARY KEY,
    origin          TEXT NOT NULL,
    destination     TEXT NOT NULL,
    flight_date     TEXT NOT NULL,
    direction       TEXT NOT NULL,
    searched_at     TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'success',
    error_message   TEXT,
    flight_count    INTEGER DEFAULT 0,
    content_hash    TEXT DEFAULT '',
    UNIQUE(origin, destination, flight_date, direction)
);

CREATE TABLE IF NOT EXISTS flights (
    id              INTEGER PRIMARY KEY,
    search_id       INTEGER NOT NULL REFERENCES searches(id) ON DELETE CASCADE,
    airline         TEXT NOT NULL,
    departure_time  TEXT NOT NULL,
    arrival_time    TEXT NOT NULL,
    depart_minutes  INTEGER DEFAULT 0,
    arrive_minutes  INTEGER DEFAULT 0,
    price           REAL NOT NULL,
    currency        TEXT DEFAULT 'GBP',
    stops           INTEGER DEFAULT 0,
    arrival_ahead   TEXT DEFAULT '',
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_searches_lookup
    ON searches(origin, destination, flight_date, direction);
"""


def _parse_time_to_minutes(time_str: str) -> int:
    """Parse flight time string to minutes from midnight.

    Handles multiple formats:
    - '6:20 PM' or '6:20 PM on Fri, Apr 17'    (12-hour, US locale)
    - '18:20 on Fri 17 Apr'                      (24-hour, UK locale)
    - '18:20'                                     (24-hour plain)
    """
    if not time_str:
        return -1
    part = time_str.split(" on ")[0].strip()

    # Try 12-hour formats
    for fmt in ("%I:%M %p", "%I:%M%p"):
        try:
            t = datetime.strptime(part, fmt)
            return t.hour * 60 + t.minute
        except ValueError:
            continue

    # Try 24-hour format (e.g. "18:20")
    try:
        t = datetime.strptime(part, "%H:%M")
        return t.hour * 60 + t.minute
    except ValueError:
        pass

    # Try extracting HH:MM from anywhere in the string
    import re
    match = re.search(r'(\d{1,2}):(\d{2})', part)
    if match:
        h, m = int(match.group(1)), int(match.group(2))
        if 0 <= h <= 23 and 0 <= m <= 59:
            return h * 60 + m

    return -1


class FlightCache:
    def __init__(self, db_path: Path = None):
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path or DB_PATH
        self.conn = sqlite3.connect(str(self.db_path), timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def upsert_airport(self, iata: str, name: str, country: str = "UK", is_origin: bool = False):
        self.conn.execute(
            "INSERT INTO airports(iata_code, name, country, is_origin) VALUES(?,?,?,?) "
            "ON CONFLICT(iata_code) DO UPDATE SET name=excluded.name, is_origin=MAX(is_origin, excluded.is_origin)",
            (iata, name, country, int(is_origin)),
        )
        self.conn.commit()

    def upsert_route(self, origin: str, destination: str, dest_name: str = ""):
        self.conn.execute(
            "INSERT INTO routes(origin, destination, dest_name) VALUES(?,?,?) "
            "ON CONFLICT(origin, destination) DO UPDATE SET dest_name=excluded.dest_name, is_active=1",
            (origin, destination, dest_name),
        )
        self.conn.commit()

    def _compute_hash(self, flights: list) -> str:
        """Compute a hash of flight data to detect changes."""
        import hashlib
        if not flights:
            return ""
        # Hash key fields: airline, times, price
        parts = sorted(
            f"{f.get('airline','')}/{f.get('departure','')}/{f.get('arrival','')}/{f.get('price',0)}"
            for f in flights
        )
        return hashlib.md5("|".join(parts).encode()).hexdigest()

    def record_search(self, origin: str, dest: str, flight_date: str, direction: str,
                      status: str = "success", error_msg: str = None, flights: list = None) -> bool:
        """Record a search. Returns True if data changed, False if unchanged."""
        now = datetime.utcnow().isoformat()
        flight_count = len(flights) if flights else 0
        new_hash = self._compute_hash(flights) if flights else ""

        # Check if data has changed
        existing = self.conn.execute(
            "SELECT content_hash FROM searches WHERE origin=? AND destination=? AND flight_date=? AND direction=?",
            (origin, dest, flight_date, direction),
        ).fetchone()

        if existing and existing["content_hash"] == new_hash and new_hash != "":
            # Data unchanged — just update the timestamp
            self.conn.execute(
                "UPDATE searches SET searched_at=? WHERE origin=? AND destination=? AND flight_date=? AND direction=?",
                (now, origin, dest, flight_date, direction),
            )
            self.conn.commit()
            return False

        # Data changed — full upsert
        self.conn.execute(
            "INSERT INTO searches(origin, destination, flight_date, direction, searched_at, status, error_message, flight_count, content_hash) "
            "VALUES(?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(origin, destination, flight_date, direction) DO UPDATE SET "
            "searched_at=excluded.searched_at, status=excluded.status, error_message=excluded.error_message, "
            "flight_count=excluded.flight_count, content_hash=excluded.content_hash",
            (origin, dest, flight_date, direction, now, status, error_msg, flight_count, new_hash),
        )

        row = self.conn.execute(
            "SELECT id FROM searches WHERE origin=? AND destination=? AND flight_date=? AND direction=?",
            (origin, dest, flight_date, direction),
        ).fetchone()
        search_id = row["id"]

        self.conn.execute("DELETE FROM flights WHERE search_id=?", (search_id,))

        if flights:
            for f in flights:
                dep_mins = _parse_time_to_minutes(f.get("departure", ""))
                arr_mins = _parse_time_to_minutes(f.get("arrival", ""))
                self.conn.execute(
                    "INSERT INTO flights(search_id, airline, departure_time, arrival_time, "
                    "depart_minutes, arrive_minutes, price, currency, stops, arrival_ahead, created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (search_id, f.get("airline", ""), f.get("departure", ""), f.get("arrival", ""),
                     dep_mins, arr_mins, f.get("price", 0), f.get("currency", "GBP"),
                     f.get("stops", 0), f.get("arrival_ahead", ""), now),
                )

        self.conn.commit()
        return True

    def get_search_age_hours(self, origin: str, dest: str, flight_date: str, direction: str) -> Optional[float]:
        row = self.conn.execute(
            "SELECT searched_at FROM searches WHERE origin=? AND destination=? AND flight_date=? AND direction=? AND status='success'",
            (origin, dest, flight_date, direction),
        ).fetchone()
        if not row:
            return None
        searched = datetime.fromisoformat(row["searched_at"])
        return (datetime.utcnow() - searched).total_seconds() / 3600

    def find_day_trips(self, origin: str, month: str, min_hours: float = 4.0,
                       max_price: float = None, destinations: list = None) -> list:
        params = [origin, f"{month}%", origin]
        dest_filter = ""
        if destinations:
            placeholders = ",".join("?" * len(destinations))
            dest_filter = f"AND so.destination IN ({placeholders})"
            params.extend(destinations)

        price_filter = ""
        if max_price:
            price_filter = "AND (of.price + rf.price) <= ?"
            params.append(max_price)

        min_time_minutes = int(min_hours * 60)

        query = f"""
        SELECT
            so.flight_date as date,
            so.destination,
            COALESCE(ro.dest_name, so.destination) as dest_name,
            of.airline as out_airline,
            of.departure_time as out_depart,
            of.arrival_time as out_arrive,
            of.arrive_minutes as out_arrive_mins,
            of.price as out_price,
            rf.airline as ret_airline,
            rf.departure_time as ret_depart,
            rf.arrival_time as ret_arrive,
            rf.depart_minutes as ret_depart_mins,
            rf.price as ret_price,
            (of.price + rf.price) as total_price,
            (rf.depart_minutes - of.arrive_minutes) as time_at_dest_minutes
        FROM flights of
        JOIN searches so ON of.search_id = so.id
        JOIN flights rf ON 1=1
        JOIN searches sr ON rf.search_id = sr.id
        LEFT JOIN routes ro ON ro.origin = so.origin AND ro.destination = so.destination
        WHERE so.origin = ?
          AND so.flight_date LIKE ?
          AND so.direction = 'outbound'
          AND so.status = 'success'
          AND sr.origin = so.destination
          AND sr.destination = ?
          {dest_filter}
          AND sr.flight_date = so.flight_date
          AND sr.direction = 'return'
          AND sr.status = 'success'
          AND of.stops = 0
          AND rf.stops = 0
          AND of.arrive_minutes > 0
          AND of.arrive_minutes < 720
          AND of.arrival_ahead = ''
          AND rf.depart_minutes > of.arrive_minutes
          AND (rf.depart_minutes - of.arrive_minutes) >= {min_time_minutes}
          {price_filter}
        ORDER BY total_price ASC
        """

        rows = self.conn.execute(query, params).fetchall()

        best = {}
        for row in rows:
            key = (row["destination"], row["date"])
            if key not in best or row["total_price"] < best[key]["total_price"]:
                best[key] = dict(row)

        return sorted(best.values(), key=lambda x: x["total_price"])

    def cleanup_expired(self):
        today = date.today().isoformat()
        self.conn.execute("DELETE FROM flights WHERE search_id IN (SELECT id FROM searches WHERE flight_date < ?)", (today,))
        self.conn.execute("DELETE FROM searches WHERE flight_date < ?", (today,))
        self.conn.commit()

    def get_stats(self) -> dict:
        return {
            "searches": self.conn.execute("SELECT COUNT(*) as c FROM searches WHERE status='success'").fetchone()["c"],
            "flights": self.conn.execute("SELECT COUNT(*) as c FROM flights").fetchone()["c"],
            "routes": self.conn.execute("SELECT COUNT(*) as c FROM routes WHERE is_active=1").fetchone()["c"],
        }
