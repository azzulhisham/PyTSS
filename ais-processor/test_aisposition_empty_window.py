"""Empty ClickHouse windows and stall detection for both position writers. No network."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pytest

import aisposition as ap
import aisposition_b as apb


NOW = datetime(2026, 11, 21, 4, 0, 0)
CATCHUP = datetime(2026, 11, 21, 1, 0, 0)
MODULES = [ap, apb]


class _Result:
    def __init__(self, column_names, result_rows):
        self.column_names = column_names
        self.result_rows = result_rows


def _columns(mod):
    if mod is ap:
        return list(mod.ROW_FIELDS)
    return list(mod.CH_FIELDS)


def _ch_row(mod, ts: datetime):
    if mod is ap:
        return (
            ts,
            563000100,
            0,
            "Under way using engine",
            103.8,
            1.26,
            0.0,
            90.0,
            12.5,
            90.0,
        )
    return (ts, 563000100, 103.8, 1.26, 90.0, 12.5, 90.0)


def _empty_shape(mod):
    if mod is ap:
        return ([], {}, 0, 0, 0)
    return ([], 0, 0, 0)


def _freeze(monkeypatch, mod, now=NOW):
    monkeypatch.setattr(mod, "_utcnow_naive", lambda: now)
    monkeypatch.setattr(mod, "WINDOW_SECONDS", 600)
    monkeypatch.setattr(mod, "LIVE_LOOKBACK_MINUTES", 10)
    monkeypatch.setattr(mod, "STALL_CYCLES", 3)
    monkeypatch.setattr(mod, "WATCHDOG_EXIT_FAILURES", 0)
    mod._consecutive_failures = 0
    mod._consecutive_stuck_catchup = 0
    mod._last_written.clear()


def _script(monkeypatch, mod, results):
    queue = list(results)

    class Client:
        def query(self, sql):
            if not queue:
                raise AssertionError("unexpected ClickHouse query")
            return queue.pop(0)

    monkeypatch.setattr(mod, "get_ch_client", lambda: Client())
    monkeypatch.setattr(mod, "write_health", lambda count: None)
    monkeypatch.setattr(mod, "upsert_positions", lambda *args, **kwargs: (1, 0))
    if mod is ap:
        monkeypatch.setattr(mod, "load_existing_positions", lambda mmsis: {})
    else:
        monkeypatch.setattr(mod, "load_existing_mmsis", lambda mmsis: set())


def _forbid_probe(monkeypatch, mod):
    def _no():
        raise AssertionError("freshness probe should not run")

    monkeypatch.setattr(mod, "_ch_max_ts", _no)


def _probe(monkeypatch, mod, max_ts, failed=False):
    monkeypatch.setattr(mod, "_ch_max_ts", lambda: (max_ts, failed))


def _stub_main(monkeypatch, mod, run_cycle):
    monkeypatch.setattr(mod, "run_cycle", run_cycle)
    monkeypatch.setattr(mod, "create_db_and_tables", lambda: None)
    monkeypatch.setattr(mod, "read_watermark", lambda: CATCHUP)
    monkeypatch.setattr(mod, "write_watermark", lambda value: None)
    monkeypatch.setattr(mod, "purge_health", lambda: 0)
    monkeypatch.setattr(mod, "drop_ch_client", lambda: None)
    monkeypatch.setattr(mod.time, "sleep", lambda seconds: None)


@pytest.mark.parametrize("mod", MODULES, ids=["class-a", "class-b"])
@pytest.mark.parametrize(
    "columns,rows",
    [
        ((), ()),
        (None, ()),
        (("mmsi",), ((123,),)),
        (("ts", "mmsi"), ()),
    ],
    ids=["no-columns", "names-none", "no-ts", "columns-no-rows"],
)
def test_empty_catchup_does_not_raise_and_advances(monkeypatch, caplog, mod, columns, rows):
    _freeze(monkeypatch, mod)
    _forbid_probe(monkeypatch, mod)
    _script(monkeypatch, mod, [_Result(columns, rows)])
    with caplog.at_level(logging.INFO):
        nxt = mod.run_cycle(CATCHUP)
    assert nxt == CATCHUP + timedelta(seconds=600)
    assert any(
        "window mode=catch-up start=2026-11-21 01:00:00 end=2026-11-21 01:10:00" in r.message
        for r in caplog.records
    )
    assert any("No rows returned (mode=catch-up)" in r.message for r in caplog.records)
    assert not any("STALL" in r.message for r in caplog.records)


@pytest.mark.parametrize("mod", MODULES, ids=["class-a", "class-b"])
def test_fetch_empty_returns_caller_shape(monkeypatch, mod):
    _script(monkeypatch, mod, [_Result((), ())])
    assert mod.fetch_positions(CATCHUP, True) == _empty_shape(mod)


@pytest.mark.parametrize("mod", MODULES, ids=["class-a", "class-b"])
def test_query_error_still_raises_and_drops_client(monkeypatch, mod):
    class Boom:
        def query(self, sql):
            raise ConnectionError("down")

    dropped = {"n": 0}

    def drop():
        dropped["n"] += 1

    monkeypatch.setattr(mod, "get_ch_client", lambda: Boom())
    monkeypatch.setattr(mod, "drop_ch_client", drop)
    with pytest.raises(ConnectionError):
        mod.fetch_positions(CATCHUP, True)
    assert dropped["n"] == 1


@pytest.mark.parametrize("mod", MODULES, ids=["class-a", "class-b"])
def test_gap_then_data_lands_on_first_row_ts(monkeypatch, mod):
    _freeze(monkeypatch, mod)
    _forbid_probe(monkeypatch, mod)
    data_ts = datetime(2026, 11, 21, 1, 30, 15)
    results = [_Result((), ())] * 3
    results.append(_Result(_columns(mod), [_ch_row(mod, data_ts)]))
    _script(monkeypatch, mod, results)
    wm = CATCHUP
    for _ in range(3):
        wm = mod.run_cycle(wm)
    assert wm == CATCHUP + timedelta(seconds=1800)
    wm = mod.run_cycle(wm)
    assert wm == data_ts


@pytest.mark.parametrize("mod", MODULES, ids=["class-a", "class-b"])
def test_advancing_empty_catchup_does_not_stall(monkeypatch, caplog, mod):
    _freeze(monkeypatch, mod)
    _forbid_probe(monkeypatch, mod)
    _script(monkeypatch, mod, [_Result((), ())] * 5)
    wm = CATCHUP
    with caplog.at_level(logging.WARNING):
        for _ in range(5):
            nxt = mod.run_cycle(wm)
            assert nxt == wm + timedelta(seconds=600)
            wm = nxt
    assert wm == CATCHUP + timedelta(seconds=3000)
    assert mod._consecutive_stuck_catchup == 0
    assert mod._consecutive_failures == 0
    assert not any("STALL" in r.message for r in caplog.records)


@pytest.mark.parametrize("mod", MODULES, ids=["class-a", "class-b"])
def test_stuck_empty_catchup_logs_stall_then_progress_resets(monkeypatch, caplog, mod):
    _freeze(monkeypatch, mod)
    _probe(monkeypatch, mod, NOW, failed=False)
    monkeypatch.setattr(mod, "WINDOW_SECONDS", 0)
    _script(monkeypatch, mod, [_Result((), ())] * 3)
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            assert mod.run_cycle(CATCHUP) == CATCHUP
    stalls = [r.message for r in caplog.records if "STALL" in r.message]
    assert len(stalls) == 1
    assert "kind=empty-catch-up" in stalls[0]
    assert "count=3" in stalls[0]
    assert "ch_max_ts=2026-11-21 04:00:00" in stalls[0]
    assert mod._consecutive_stuck_catchup == 3

    caplog.clear()
    monkeypatch.setattr(mod, "WINDOW_SECONDS", 600)
    data_ts = datetime(2026, 11, 21, 1, 5, 0)
    _script(monkeypatch, mod, [_Result(_columns(mod), [_ch_row(mod, data_ts)])])
    with caplog.at_level(logging.WARNING):
        nxt = mod.run_cycle(CATCHUP)
    assert nxt == data_ts
    assert mod._consecutive_stuck_catchup == 0
    assert mod._consecutive_failures == 0
    assert not any("STALL" in r.message for r in caplog.records)

    caplog.clear()
    monkeypatch.setattr(mod, "WINDOW_SECONDS", 0)
    _script(monkeypatch, mod, [_Result((), ())] * 2)
    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            mod.run_cycle(CATCHUP)
    assert mod._consecutive_stuck_catchup == 2
    assert not any("STALL" in r.message for r in caplog.records)


@pytest.mark.parametrize("mod", MODULES, ids=["class-a", "class-b"])
def test_stuck_stall_skipped_when_source_is_stale(monkeypatch, caplog, mod):
    _freeze(monkeypatch, mod)
    _probe(monkeypatch, mod, NOW - timedelta(hours=2), failed=False)
    monkeypatch.setattr(mod, "WINDOW_SECONDS", 0)
    _script(monkeypatch, mod, [_Result((), ())] * 3)
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            mod.run_cycle(CATCHUP)
    assert mod._consecutive_stuck_catchup == 3
    assert not any("STALL" in r.message for r in caplog.records)


@pytest.mark.parametrize("mod", MODULES, ids=["class-a", "class-b"])
def test_stuck_stall_when_freshness_probe_fails(monkeypatch, caplog, mod):
    _freeze(monkeypatch, mod)
    _probe(monkeypatch, mod, None, failed=True)
    monkeypatch.setattr(mod, "WINDOW_SECONDS", 0)
    _script(monkeypatch, mod, [_Result((), ())] * 3)
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            assert mod.run_cycle(CATCHUP) == CATCHUP
    stalls = [r.message for r in caplog.records if "STALL" in r.message]
    assert len(stalls) == 1
    assert "freshness=check_failed" in stalls[0]


@pytest.mark.parametrize("mod", MODULES, ids=["class-a", "class-b"])
def test_live_inserted_zero_does_not_stall_and_resets(monkeypatch, caplog, mod):
    _freeze(monkeypatch, mod)
    _forbid_probe(monkeypatch, mod)
    live_wm = NOW - timedelta(minutes=1)
    row_ts = NOW - timedelta(seconds=30)
    _script(monkeypatch, mod, [_Result(_columns(mod), [_ch_row(mod, row_ts)])])
    mod._consecutive_failures = 2
    mod._consecutive_stuck_catchup = 2
    with caplog.at_level(logging.INFO):
        nxt = mod.run_cycle(live_wm)
    assert nxt == row_ts
    assert mod._consecutive_failures == 0
    assert mod._consecutive_stuck_catchup == 0
    assert not any("STALL" in r.message for r in caplog.records)
    assert any("window mode=live" in r.message for r in caplog.records)
    assert any("inserted=0" in r.message for r in caplog.records)


@pytest.mark.parametrize("mod", MODULES, ids=["class-a", "class-b"])
def test_consecutive_failures_log_stall(monkeypatch, caplog, mod):
    _freeze(monkeypatch, mod)
    _probe(monkeypatch, mod, NOW, failed=False)
    calls = {"n": 0}

    def run_cycle(wm):
        calls["n"] += 1
        if calls["n"] <= 3:
            raise RuntimeError("cycle broke")
        raise KeyboardInterrupt()

    _stub_main(monkeypatch, mod, run_cycle)
    with caplog.at_level(logging.WARNING):
        mod.main()
    stalls = [r.message for r in caplog.records if "STALL" in r.message]
    assert stalls == [
        "STALL kind=failure mode=catch-up count=3 watermark=2026-11-21 01:00:00 "
        "ch_max_ts=2026-11-21 04:00:00"
    ]
    assert mod._consecutive_failures == 3


@pytest.mark.parametrize("mod", MODULES, ids=["class-a", "class-b"])
def test_watchdog_exits_after_consecutive_failures(monkeypatch, mod):
    _freeze(monkeypatch, mod)
    monkeypatch.setattr(mod, "WATCHDOG_EXIT_FAILURES", 2)
    monkeypatch.setattr(mod, "STALL_CYCLES", 10)
    _forbid_probe(monkeypatch, mod)

    def run_cycle(wm):
        raise RuntimeError("cycle broke")

    _stub_main(monkeypatch, mod, run_cycle)
    with pytest.raises(SystemExit) as exc:
        mod.main()
    assert exc.value.code == 1
    assert mod._consecutive_failures == 2
