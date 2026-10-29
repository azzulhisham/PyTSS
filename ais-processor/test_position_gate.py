"""Gates for the position writers. No database."""

from datetime import datetime, timedelta
from pathlib import Path

from position_gate import (
    COG_NA_DEG,
    SOG_NA_KNOTS,
    apply_stationary_throttle,
    prepare_position,
    remember_written,
)

MOVE_M = 10.0
REFRESH_S = 120.0
SOG_KN = 0.5


def moored(ts, cog=360.0, sog=0.0, lat=3.2, lon=113.0, nav=5, heading=511):
    return (ts, 677068800, nav, "moored", lon, lat, 0.0, cog, sog, heading)


def test_moored_cog_360_is_kept_as_the_sentinel_number():
    ts = datetime(2026, 10, 29, 6, 16)
    row, invalid, sentinel = prepare_position(moored(ts))
    assert row is not None
    assert invalid is False
    assert sentinel is True
    assert row[0] == ts
    assert row[4] == 113.0
    assert row[5] == 3.2
    assert row[7] == 360.0
    assert row[8] == 0.0
    assert row[9] == 511


def test_lat_91_lon_181_is_skipped():
    row, invalid, sentinel = prepare_position(
        moored(datetime(2026, 10, 29, 6, 16), lat=91.0, lon=181.0)
    )
    assert row is None
    assert invalid is True
    assert sentinel is False


def test_normal_row_is_unchanged():
    raw = moored(datetime(2026, 10, 29, 1, 0), cog=40.0, sog=8.2, heading=90)
    row, invalid, sentinel = prepare_position(raw)
    assert row == raw
    assert invalid is False
    assert sentinel is False


def test_missing_course_is_filled_because_the_column_is_not_null():
    row, invalid, sentinel = prepare_position(moored(datetime(2026, 10, 29), cog=None))
    assert invalid is False
    assert sentinel is True
    assert row[7] == COG_NA_DEG
    assert row[8] == 0.0


def test_sog_not_available_is_kept():
    row, invalid, sentinel = prepare_position(moored(datetime(2026, 10, 29), sog=102.2))
    assert invalid is False
    assert sentinel is True
    assert row[8] == SOG_NA_KNOTS
    assert row[7] == 360.0


def test_true_heading_511_is_left_alone():
    raw = moored(datetime(2026, 10, 29), cog=12.0, sog=4.0, heading=511)
    row, invalid, sentinel = prepare_position(raw)
    assert row[9] == 511
    assert sentinel is False
    assert invalid is False


def test_stationary_ship_reporting_every_3_minutes_still_updates():
    state = {}
    start = datetime(2026, 10, 29, 8, 0)
    for step in range(4):
        report = moored(start + timedelta(minutes=3 * step))
        kept, skipped = apply_stationary_throttle(
            [report], state, move_m=MOVE_M, refresh_s=REFRESH_S, sog_kn=SOG_KN
        )
        assert skipped == 0
        assert kept == [report]
        remember_written(state, kept)


def test_stationary_ship_reporting_every_10_seconds_updates_at_most_every_120():
    state = {}
    start = datetime(2026, 10, 29, 8, 0)
    writes = []
    for step in range(0, 301, 10):
        report = moored(start + timedelta(seconds=step))
        kept, skipped = apply_stationary_throttle(
            [report], state, move_m=MOVE_M, refresh_s=REFRESH_S, sog_kn=SOG_KN
        )
        if kept:
            assert skipped == 0
            remember_written(state, kept)
            writes.append(step)
        else:
            assert skipped == 1
    assert writes == [0, 120, 240]


def test_a_real_move_status_change_or_speed_is_written_immediately():
    state = {}
    start = datetime(2026, 10, 29, 8, 0)
    remember_written(state, [moored(start)])
    later = start + timedelta(seconds=10)

    moved = moored(later, lat=3.2002)
    kept, skipped = apply_stationary_throttle(
        [moved], state, move_m=MOVE_M, refresh_s=REFRESH_S, sog_kn=SOG_KN
    )
    assert kept == [moved]
    assert skipped == 0

    status = moored(later, nav=0)
    kept, skipped = apply_stationary_throttle(
        [status], state, move_m=MOVE_M, refresh_s=REFRESH_S, sog_kn=SOG_KN
    )
    assert kept == [status]
    assert skipped == 0

    underway = moored(later, sog=0.6, cog=40.0)
    kept, skipped = apply_stationary_throttle(
        [underway], state, move_m=MOVE_M, refresh_s=REFRESH_S, sog_kn=SOG_KN
    )
    assert kept == [underway]
    assert skipped == 0


def test_a_ship_that_just_stopped_is_written():
    state = {}
    start = datetime(2026, 10, 29, 8, 0)
    remember_written(state, [moored(start, cog=40.0, sog=8.0)])
    stopped = moored(start + timedelta(seconds=10), cog=360.0, sog=0.0)
    kept, skipped = apply_stationary_throttle(
        [stopped], state, move_m=MOVE_M, refresh_s=REFRESH_S, sog_kn=SOG_KN
    )
    assert kept == [stopped]
    assert skipped == 0


def test_writers_count_invalid_positions_and_field_sentinels_apart():
    folder = Path(__file__).resolve().parent
    writer = (folder / "aisposition.py").read_text()
    class_b = (folder / "aisposition_b.py").read_text()
    assert "_is_itu_sentinel" not in writer
    assert "field_sentinel=%s" in writer
    assert "stationary_throttled=%s" in writer
    assert "STATIONARY_REFRESH_SEC" in writer
    assert "prepare_position" in class_b
    assert "field_sentinel=%s" in class_b
    assert "stationary_throttled=%s" in class_b
