"""Adaptive rate limiter — starts fast, slows only when Google pushes back."""
from __future__ import annotations

import logging
import random
import time
from threading import Lock

from config import (
    BACKOFF_INITIAL, BACKOFF_MULTIPLIER, BACKOFF_MAX,
    MAX_CONSECUTIVE_ERRORS,
)

logger = logging.getLogger(__name__)

# Adaptive speed levels
LEVELS = [
    {"name": "aggressive", "min": 0.5, "max": 1.0},
    {"name": "fast",       "min": 1.0, "max": 2.0},
    {"name": "normal",     "min": 1.5, "max": 3.0},
    {"name": "cautious",   "min": 3.0, "max": 5.0},
    {"name": "slow",       "min": 5.0, "max": 8.0},
]

# How many consecutive successes before speeding up
SPEEDUP_THRESHOLD = 30


class RateLimiter:
    def __init__(self):
        self._lock = Lock()
        self._last_request = 0.0
        self._request_count = 0
        self._consecutive_errors = 0
        self._consecutive_successes = 0
        self._current_backoff = BACKOFF_INITIAL
        self._aborted = False
        self._level_idx = 0  # start at aggressive
        self._level_changes = []

    @property
    def is_aborted(self) -> bool:
        return self._aborted

    @property
    def request_count(self) -> int:
        return self._request_count

    @property
    def current_level(self) -> str:
        return LEVELS[self._level_idx]["name"]

    def _slow_down(self):
        """Move to a slower speed level."""
        if self._level_idx < len(LEVELS) - 1:
            self._level_idx += 1
            lvl = LEVELS[self._level_idx]
            logger.info(f"Rate limiter: slowing to '{lvl['name']}' ({lvl['min']}-{lvl['max']}s)")
            self._level_changes.append((self._request_count, "slower", lvl["name"]))

    def _speed_up(self):
        """Move to a faster speed level."""
        if self._level_idx > 0:
            self._level_idx -= 1
            lvl = LEVELS[self._level_idx]
            logger.info(f"Rate limiter: speeding up to '{lvl['name']}' ({lvl['min']}-{lvl['max']}s)")
            self._level_changes.append((self._request_count, "faster", lvl["name"]))

    def wait(self):
        with self._lock:
            if self._aborted:
                raise AbortError("Too many consecutive errors")

            lvl = LEVELS[self._level_idx]
            now = time.time()
            elapsed = now - self._last_request
            delay = random.uniform(lvl["min"], lvl["max"])
            if elapsed < delay:
                time.sleep(delay - elapsed)

            self._last_request = time.time()
            self._request_count += 1

    def record_success(self):
        self._consecutive_errors = 0
        self._current_backoff = BACKOFF_INITIAL
        self._consecutive_successes += 1

        # Speed up after sustained success
        if self._consecutive_successes >= SPEEDUP_THRESHOLD:
            self._speed_up()
            self._consecutive_successes = 0

    def record_error(self, is_rate_limit: bool = False):
        self._consecutive_errors += 1
        self._consecutive_successes = 0

        if self._consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            logger.error(f"Aborting: {self._consecutive_errors} consecutive errors")
            self._aborted = True
            return

        # Slow down on any error
        self._slow_down()

        if is_rate_limit:
            # Extra slow down for rate limits
            self._slow_down()
            backoff = self._current_backoff
            self._current_backoff = min(self._current_backoff * BACKOFF_MULTIPLIER, BACKOFF_MAX)
        else:
            backoff = min(self._current_backoff, 60)

        jitter = random.uniform(0, backoff * 0.3)
        total_wait = backoff + jitter
        logger.warning(f"Error backoff: {total_wait:.0f}s (attempt {self._consecutive_errors}/{MAX_CONSECUTIVE_ERRORS})")
        time.sleep(total_wait)

    def report(self) -> str:
        """Return a summary of rate limiter behaviour."""
        lvl = LEVELS[self._level_idx]
        lines = [
            f"  Final speed level: {lvl['name']} ({lvl['min']}-{lvl['max']}s)",
            f"  Level changes:     {len(self._level_changes)}",
        ]
        for req, direction, name in self._level_changes:
            lines.append(f"    at request {req}: {direction} -> {name}")
        return "\n".join(lines)


class AbortError(Exception):
    pass
