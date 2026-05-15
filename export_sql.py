#!/usr/bin/env python3
"""Export local SQLite flight cache to a SQL dump file for D1 bulk import.

Only exports searches where the content_hash has changed since the last export.
Stores previous hashes in a JSON file so subsequent runs skip unchanged data.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".flightcache"
DB_PATH = CACHE_DIR / "flights.db"
DUMP_PATH = CACHE_DIR / "d1_import.sql"
HASH_PATH = CACHE_DIR / "previous_hashes.json"


def escape_sql(val) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    s = str(val).replace("'", "''")
    return f"'{s}'"


def stable_search_id(origin: str, destination: str, flight_date: str, direction: str) -> int:
    """Compute a deterministic 63-bit positive INTEGER search_id from the
    UNIQUE key. Same key always produces the same id — eliminates the
    `(SELECT id FROM searches WHERE ...)` subquery that flight inserts
    previously paid (one indexed read per flight). See card #385 option 5.

    Birthday-collision odds with 826k rows in a 63-bit space: ~3.7e-8.
    """
    import hashlib as _h
    digest = _h.sha256(f"{origin}|{destination}|{flight_date}|{direction}".encode()).digest()
    return int.from_bytes(digest[:8], 'big') & 0x7FFFFFFFFFFFFFFF


def strip_date_suffix(val):
    """Drop "on Sun 1 Nov"-style suffix from flight time strings — the date is
    already implied by searches.flight_date, so storing it on every row is waste.
    See card #367 item #4."""
    if val is None:
        return None
    s = str(val)
    return s.split(" on ", 1)[0] if " on " in s else s


def load_previous_hashes() -> dict:
    """Load content hashes from the previous run."""
    if HASH_PATH.exists():
        try:
            with open(HASH_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_current_hashes(hashes: dict):
    """Save current content hashes for next run comparison."""
    with open(HASH_PATH, "w") as f:
        json.dump(hashes, f)


def export(db_path: Path = DB_PATH, dump_path: Path = DUMP_PATH) -> Path:
    local = sqlite3.connect(str(db_path))
    local.row_factory = sqlite3.Row

    previous_hashes = load_previous_hashes()
    current_hashes = {}
    skipped_unchanged = 0
    exported_searches = 0
    exported_flights = 0

    # Collect all searches and determine which changed
    all_searches = local.execute("SELECT * FROM searches").fetchall()
    changed_searches = []
    skipped_no_results = 0
    for s in all_searches:
        key = f"{s['origin']}|{s['destination']}|{s['flight_date']}|{s['direction']}"
        content_hash = s['content_hash'] if 'content_hash' in s.keys() else ''
        current_hashes[key] = content_hash

        prev_hash = previous_hashes.get(key)
        # Skip if hash matches (including both being empty = no flights found both times)
        if (content_hash == prev_hash) and prev_hash is not None:
            skipped_unchanged += 1
            continue

        # Skip syncing no_results / error rows to D1 (card #385 option 1).
        # The Worker's read paths all filter status='success', and these rows
        # accumulated to 62% of the searches table before being pruned. Local
        # SQLite still records them so the hash-skip on subsequent runs is
        # unaffected.
        if s['status'] != 'success':
            skipped_no_results += 1
            continue

        changed_searches.append(s)
        # Log why it changed (first 5 only to avoid spam)
        if len(changed_searches) <= 5:
            if not prev_hash:
                logger.info(f"  CHANGED {key}: no previous hash (first run for this search)")
            elif not content_hash:
                logger.info(f"  CHANGED {key}: no current hash (no flights found)")
            else:
                logger.info(f"  CHANGED {key}: hash {prev_hash[:8]}→{content_hash[:8]}")

    logger.info(f"Previous hashes loaded: {len(previous_hashes)}")
    # Show sample of previous keys vs current keys to debug mismatches
    prev_keys = set(previous_hashes.keys())
    curr_keys = set(current_hashes.keys())
    matched_keys = prev_keys & curr_keys
    new_keys = curr_keys - prev_keys
    gone_keys = prev_keys - curr_keys
    logger.info(f"Keys: {len(matched_keys)} matched, {len(new_keys)} new, {len(gone_keys)} gone from previous")
    if new_keys:
        for k in sorted(new_keys)[:3]:
            logger.info(f"  NEW KEY: {k}")
    if gone_keys:
        for k in sorted(gone_keys)[:3]:
            logger.info(f"  GONE KEY: {k}")
    # For matched keys, show how many have same hash
    hash_matches = sum(1 for k in matched_keys if current_hashes[k] == previous_hashes[k])
    hash_diffs = len(matched_keys) - hash_matches
    logger.info(f"Matched keys: {hash_matches} same hash, {hash_diffs} hash changed")
    logger.info(f"Searches: {len(all_searches)} total, {len(changed_searches)} changed, {skipped_unchanged} unchanged (skipped), {skipped_no_results} non-success (skipped, not synced to D1)")

    if not changed_searches:
        # Nothing changed — write minimal SQL
        with open(dump_path, "w") as f:
            f.write("-- No changes detected, nothing to import\nSELECT 1;\n")
        save_current_hashes(current_hashes)
        local.close()
        logger.info("No changes — empty SQL dump")
        return dump_path

    # Compute reference-table hashes so we can skip emitting unchanged
    # airports/routes (card #385 option 3). We hash only the columns we'd
    # actually write — last_scraped on routes is intentionally excluded so a
    # timestamp-only refresh doesn't force a re-write.
    import hashlib as _hashlib
    airports = local.execute("SELECT * FROM airports").fetchall()
    routes = local.execute("SELECT * FROM routes").fetchall()
    airports_hash = _hashlib.md5(
        "|".join(f"{a['iata_code']}/{a['name']}/{a['country']}/{a['is_origin']}" for a in airports).encode()
    ).hexdigest()
    routes_hash = _hashlib.md5(
        "|".join(f"{r['origin']}/{r['destination']}/{r['dest_name']}/{r['is_active']}" for r in routes).encode()
    ).hexdigest()
    prev_airports_hash = previous_hashes.get("__airports__")
    prev_routes_hash = previous_hashes.get("__routes__")
    current_hashes["__airports__"] = airports_hash
    current_hashes["__routes__"] = routes_hash

    with open(dump_path, "w") as f:
        # Ensure indexes and history table exist (compacted shape — see #367 #5).
        f.write("CREATE INDEX IF NOT EXISTS idx_flights_search_id ON flights(search_id);\n")
        f.write("CREATE TABLE IF NOT EXISTS price_history (\n"
                "  id INTEGER PRIMARY KEY, origin TEXT NOT NULL, destination TEXT NOT NULL,\n"
                "  flight_date TEXT NOT NULL, direction TEXT NOT NULL,\n"
                "  price REAL NOT NULL, recorded_at TEXT NOT NULL\n);\n\n")

        # Upsert airports — only emit if the snapshot changed (#385 option 3),
        # and use a qualified ON CONFLICT WHERE clause so even when emitted, no
        # row_writes are charged for rows whose values are identical (#385 option 7).
        if airports_hash != prev_airports_hash:
            for a in airports:
                f.write(
                    f"INSERT INTO airports(iata_code, name, country, is_origin) VALUES("
                    f"{escape_sql(a['iata_code'])}, {escape_sql(a['name'])}, "
                    f"{escape_sql(a['country'])}, {a['is_origin']}) "
                    f"ON CONFLICT(iata_code) DO UPDATE SET name=excluded.name, "
                    f"country=excluded.country, is_origin=MAX(is_origin, excluded.is_origin) "
                    f"WHERE airports.name != excluded.name "
                    f"OR airports.country != excluded.country "
                    f"OR airports.is_origin < excluded.is_origin;\n"
                )
            logger.info(f"Exported {len(airports)} airports (hash changed)")
        else:
            logger.info(f"Skipped {len(airports)} airports (hash unchanged)")

        # Upsert routes — same hash-skip + qualified UPSERT pattern.
        if routes_hash != prev_routes_hash:
            for r in routes:
                f.write(
                    f"INSERT INTO routes(origin, destination, dest_name, is_active, last_scraped) VALUES("
                    f"{escape_sql(r['origin'])}, {escape_sql(r['destination'])}, "
                    f"{escape_sql(r['dest_name'])}, {r['is_active']}, datetime('now')) "
                    f"ON CONFLICT(origin, destination) DO UPDATE SET "
                    f"dest_name=excluded.dest_name, is_active=excluded.is_active, last_scraped=datetime('now') "
                    f"WHERE routes.dest_name != excluded.dest_name "
                    f"OR routes.is_active != excluded.is_active;\n"
                )
            f.write("\n")
            logger.info(f"Exported {len(routes)} routes (hash changed)")
        else:
            logger.info(f"Skipped {len(routes)} routes (hash unchanged)")

        # Only delete and re-insert CHANGED searches.
        # Per-search emission shape (#385 options 5 + 2):
        #   (optional) INSERT INTO price_history ...
        #   DELETE FROM flights WHERE search_id IN (...by content key — also
        #     catches legacy auto-id rows being replaced by hash-id ones)
        #   DELETE FROM searches WHERE origin=? AND destination=? AND flight_date=?
        #   INSERT INTO searches(id, ...) VALUES(<stable_id>, ...)
        #   INSERT INTO flights(search_id, ...) VALUES(<stable_id>, ...), (<stable_id>, ...)
        # All flights for the search are batched into ONE multi-row INSERT,
        # using the explicit deterministic id — no FK subquery needed.
        history_count = 0
        for s in changed_searches:
            o, d, fd = s["origin"], s["destination"], s["flight_date"]
            direction = s["direction"]
            searched_at = s["searched_at"]
            sid = stable_search_id(o, d, fd, direction)

            # Get flights for this search
            flights = local.execute("""
                SELECT f.* FROM flights f WHERE f.search_id = ?
            """, (s["id"],)).fetchall()

            # Log only the cheapest price per (route, date, direction, scrape-day)
            # to price_history — append-only, never deleted. The price-history
            # modal only ever uses MIN(price) per scrape window, so storing
            # every flight option was wasted writes (#367 item #5).
            #
            # Additionally skip the write when the cheapest price hasn't changed
            # since the last emit for this quad — ~17% of historical rows were
            # flat-price duplicates. Tracked via previous_hashes.json (PH: prefix
            # to avoid collision with content_hash keys). See card #403.
            if flights:
                cheapest_price = min(fl["price"] for fl in flights)
                ph_key = f"PH:{o}|{d}|{fd}|{direction}"
                prev_price = previous_hashes.get(ph_key)
                current_hashes[ph_key] = cheapest_price
                if prev_price != cheapest_price:
                    f.write(
                        f"INSERT INTO price_history(origin, destination, flight_date, direction, "
                        f"price, recorded_at) VALUES("
                        f"{escape_sql(o)}, {escape_sql(d)}, {escape_sql(fd)}, {escape_sql(direction)}, "
                        f"{cheapest_price}, {escape_sql(searched_at)});\n"
                    )
                    history_count += 1

            # Delete old data for this specific search. The DELETE FROM flights
            # uses the by-content-key subquery so legacy auto-id flight rows
            # still get cleaned up during the auto-id -> hash-id transition.
            f.write(f"DELETE FROM flights WHERE search_id IN "
                    f"(SELECT id FROM searches WHERE origin={escape_sql(o)} "
                    f"AND destination={escape_sql(d)} AND flight_date={escape_sql(fd)});\n")
            f.write(f"DELETE FROM searches WHERE origin={escape_sql(o)} "
                    f"AND destination={escape_sql(d)} AND flight_date={escape_sql(fd)};\n")

            # Insert search with explicit deterministic id. content_hash dropped
            # from D1 in card #403 — it was unused by the Worker and only useful
            # locally where it lives in previous_hashes.json.
            f.write(
                f"INSERT INTO searches(id, origin, destination, flight_date, direction, "
                f"searched_at, status, error_message) VALUES("
                f"{sid}, {escape_sql(s['origin'])}, {escape_sql(s['destination'])}, "
                f"{escape_sql(s['flight_date'])}, {escape_sql(s['direction'])}, "
                f"{escape_sql(s['searched_at'])}, {escape_sql(s['status'])}, "
                f"{escape_sql(s['error_message'])});\n"
            )
            exported_searches += 1

            # Multi-row INSERT for this search's flights — single statement
            # using the explicit search_id, no FK subquery.
            if flights:
                values_rows = []
                for fl in flights:
                    values_rows.append(
                        f"({sid}, {escape_sql(fl['airline'])}, "
                        f"{escape_sql(strip_date_suffix(fl['departure_time']))}, "
                        f"{escape_sql(strip_date_suffix(fl['arrival_time']))}, "
                        f"{fl['depart_minutes']}, {fl['arrive_minutes']}, "
                        f"{fl['price']}, {fl['stops']}, "
                        f"{escape_sql(fl['arrival_ahead'])})"
                    )
                    exported_flights += 1
                f.write(
                    "INSERT INTO flights(search_id, airline, departure_time, arrival_time, "
                    "depart_minutes, arrive_minutes, price, stops, arrival_ahead) VALUES\n  "
                    + ",\n  ".join(values_rows) + ";\n"
                )

    # Save hashes for next run
    save_current_hashes(current_hashes)

    local.close()
    size_kb = dump_path.stat().st_size / 1024
    logger.info(f"Exported {exported_searches} searches, {exported_flights} flights, {history_count} price history records ({size_kb:.0f} KB)")
    logger.info(f"Skipped {skipped_unchanged} unchanged searches")

    # Write export stats JSON for CI reporting
    export_stats = {
        "exported_searches": exported_searches,
        "exported_flights": exported_flights,
        "skipped_unchanged": skipped_unchanged,
        "changed_searches": len(changed_searches),
        "total_searches": len(all_searches),
        "history_records": history_count,
        "size_kb": round(size_kb, 1),
    }
    stats_path = dump_path.parent / "export_stats.json"
    with open(stats_path, "w") as f:
        json.dump(export_stats, f)

    return dump_path


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    db_path = Path(os.environ.get("DB_PATH", str(DB_PATH)))
    dump_path = Path(os.environ.get("DUMP_PATH", str(DUMP_PATH)))

    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        return 1

    export(db_path, dump_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
