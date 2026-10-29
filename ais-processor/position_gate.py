"""Pure gates for the AIS position writers. No database access.

A row is dropped only when its position is missing or outside the valid
range. COG 360 and SOG 102.2 mean "not available" in AIS, but
public.ais_position.cog and public.ais_position.sog are NOT NULL, so those
numbers are stored as-is and the row is kept.

Stationary repeats are judged against the writer's own last-written memory.
There is no Postgres read here.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Optional, Sequence

SOG_NA_KNOTS = 102.2
COG_NA_DEG = 360.0

# Shared row layout: ts, mmsi, navStatus, navStatusDesc, lon, lat, rot, cog, sog, trueHeading.
I_TS = 0
I_MMSI = 1
I_NAV = 2
I_LON = 4
I_LAT = 5
I_COG = 7
I_SOG = 8


def _num(value) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _naive(value: datetime) -> datetime:
    if getattr(value, "tzinfo", None) is not None:
        return value.replace(tzinfo=None)
    return value


def prepare_position(values: Sequence) -> tuple[tuple | None, bool, bool]:
    """Return ``(row, position_invalid, field_sentinel)``.

    ``position_invalid`` drops the row. ``field_sentinel`` means course or
    speed was "not available": a missing value is filled with 360 or 102.2
    so the NOT NULL columns can be written, and an existing sentinel number
    is left unchanged.
    """
    if len(values) <= I_SOG:
        return None, True, False

    lat = _num(values[I_LAT])
    lon = _num(values[I_LON])
    if lat is None or lon is None or lat < -90.0 or lat > 90.0 or lon < -180.0 or lon > 180.0:
        return None, True, False

    cog = _num(values[I_COG])
    sog = _num(values[I_SOG])
    sentinel = False
    replacements: dict[int, float] = {}
    if cog is None:
        sentinel = True
        replacements[I_COG] = COG_NA_DEG
    elif cog >= COG_NA_DEG:
        sentinel = True
    if sog is None:
        sentinel = True
        replacements[I_SOG] = SOG_NA_KNOTS
    elif sog >= SOG_NA_KNOTS:
        sentinel = True

    if not replacements:
        return tuple(values), False, sentinel

    row = list(values)
    for index, number in replacements.items():
        row[index] = number
    return tuple(row), False, sentinel


def distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_m = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * radius_m * math.asin(min(1.0, math.sqrt(a)))


def _real_sog(value) -> Optional[float]:
    number = _num(value)
    if number is None or number >= SOG_NA_KNOTS:
        return None
    return number


def stationary_throttled(
    state: dict,
    row: Sequence,
    *,
    move_m: float,
    refresh_s: float,
    sog_kn: float,
) -> bool:
    """True when this repeat should not be written.

    Memory holds the last row this process actually wrote. A ship is
    stationary when it is within ``move_m`` metres, nav status is unchanged,
    and both speeds are under ``sog_kn`` (or not a real speed). It is written
    again once the stored timestamp is at least ``refresh_s`` older.
    """
    previous = state.get(int(row[I_MMSI]))
    if previous is None:
        return False

    prev_ts, prev_lat, prev_lon, prev_nav, prev_sog = previous
    new_ts = row[I_TS]
    if not isinstance(new_ts, datetime) or not isinstance(prev_ts, datetime):
        return False

    new_sog = _real_sog(row[I_SOG])
    old_sog = _real_sog(prev_sog)
    if new_sog is not None and new_sog >= sog_kn:
        return False
    if old_sog is not None and old_sog >= sog_kn:
        return False
    if row[I_NAV] != prev_nav:
        return False

    new_lat = _num(row[I_LAT])
    new_lon = _num(row[I_LON])
    if new_lat is None or new_lon is None:
        return False
    if distance_m(prev_lat, prev_lon, new_lat, new_lon) > move_m:
        return False

    age = (_naive(new_ts) - _naive(prev_ts)).total_seconds()
    return age < refresh_s


def remember_written(state: dict, rows: Sequence[Sequence]) -> None:
    """Record rows that were sent to Postgres. Throttled rows stay out."""
    for row in rows:
        lat = _num(row[I_LAT])
        lon = _num(row[I_LON])
        ts = row[I_TS]
        if lat is None or lon is None or not isinstance(ts, datetime):
            continue
        state[int(row[I_MMSI])] = (_naive(ts), lat, lon, row[I_NAV], _num(row[I_SOG]))


def apply_stationary_throttle(
    rows: Sequence[Sequence],
    state: dict,
    *,
    move_m: float,
    refresh_s: float,
    sog_kn: float,
) -> tuple[list, int]:
    kept: list = []
    skipped = 0
    for row in rows:
        if stationary_throttled(state, row, move_m=move_m, refresh_s=refresh_s, sog_kn=sog_kn):
            skipped += 1
            continue
        kept.append(row)
    return kept, skipped
