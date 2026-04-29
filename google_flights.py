"""Google Flights search via fast-flights with consent cookie bypass."""
from __future__ import annotations

import logging
import random
from typing import Optional

from config import CONSENT_COOKIES, CHROME_VERSIONS

logger = logging.getLogger(__name__)

_default_cookie = CONSENT_COOKIES[0]
_default_chrome = CHROME_VERSIONS[0]


def _get_patched_fetch(cookie_str: str, chrome_version: str):
    """Create a fetch function with cookies, TLS fingerprint, and forced UK locale."""
    from primp import Client

    def _fetch(params):
        # Force GBP currency and UK English locale
        params["curr"] = "GBP"
        params["hl"] = "en-GB"
        params["gl"] = "uk"

        client = Client(impersonate=chrome_version, verify=False)
        res = client.get(
            "https://www.google.com/travel/flights",
            params=params,
            headers={
                "Cookie": cookie_str,
                "Accept-Language": "en-GB,en;q=0.9",
            },
        )
        assert res.status_code == 200, f"{res.status_code}"
        return res

    return _fetch


def search_flights(
    from_airport: str,
    to_airport: str,
    date: str,
    adults: int = 1,
    seat: str = "economy",
    max_stops: int = None,
    cookie_str: str = None,
    chrome_version: str = None,
) -> Optional[object]:
    """
    Search Google Flights for one-way flights.

    Returns a fast_flights Result object, or None on failure.
    """
    import fast_flights.core as _core
    from fast_flights import FlightData, Passengers, get_flights

    cookie = cookie_str or _default_cookie
    chrome = chrome_version or _default_chrome
    original_fetch = _core.fetch
    _core.fetch = _get_patched_fetch(cookie, chrome)

    try:
        result = get_flights(
            flight_data=[FlightData(date=date, from_airport=from_airport, to_airport=to_airport)],
            trip="one-way",
            seat=seat,
            passengers=Passengers(adults=adults),
            max_stops=max_stops,
        )
        return result
    except RuntimeError as e:
        if "No flights found" in str(e):
            return None
        raise
    finally:
        _core.fetch = original_fetch
