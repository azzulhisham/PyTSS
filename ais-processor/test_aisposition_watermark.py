"""Watermark hardening for aisposition.py. No network."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import aisposition as ap


TS = datetime(2026, 10, 27, 12, 34, 56)
TS2 = datetime(2026, 10, 27, 13, 0, 0)


def _point(tmp_path: Path, monkeypatch):
    path = tmp_path / "pnav_aisposition.txt"
    monkeypatch.setattr(ap, "WATERMARK_PATH", path)
    return path


def test_atomic_write_creates_main_and_bak(tmp_path, monkeypatch):
    path = _point(tmp_path, monkeypatch)
    ap.write_watermark(TS)
    assert path.is_file()
    assert path.read_text(encoding="utf-8").strip() == TS.strftime(ap.TS_FORMAT)
    assert not Path(f"{path}.tmp").exists()

    ap.write_watermark(TS2)
    bak = Path(f"{path}.bak")
    assert bak.is_file()
    assert bak.read_text(encoding="utf-8").strip() == TS.strftime(ap.TS_FORMAT)
    assert path.read_text(encoding="utf-8").strip() == TS2.strftime(ap.TS_FORMAT)


def test_none_is_not_written(tmp_path, monkeypatch, caplog):
    path = _point(tmp_path, monkeypatch)
    ap.write_watermark(TS)
    before = path.read_text(encoding="utf-8")
    with caplog.at_level(logging.DEBUG):
        ap.write_watermark(None)
    assert path.read_text(encoding="utf-8") == before
    assert any("Skipping watermark write" in r.message for r in caplog.records)


def test_fallback_to_bak_when_main_missing(tmp_path, monkeypatch, caplog):
    path = _point(tmp_path, monkeypatch)
    bak = Path(f"{path}.bak")
    bak.write_text(f"{TS.strftime(ap.TS_FORMAT)}\n", encoding="utf-8")
    with caplog.at_level(logging.WARNING):
        got = ap.read_watermark()
    assert got == TS
    assert any("using backup" in r.message.lower() for r in caplog.records)


def test_fallback_to_bak_when_main_empty(tmp_path, monkeypatch, caplog):
    path = _point(tmp_path, monkeypatch)
    path.write_text("\n", encoding="utf-8")
    bak = Path(f"{path}.bak")
    bak.write_text(f"{TS.strftime(ap.TS_FORMAT)}\n", encoding="utf-8")
    with caplog.at_level(logging.WARNING):
        got = ap.read_watermark()
    assert got == TS


def test_fallback_to_bak_when_main_corrupt(tmp_path, monkeypatch, caplog):
    path = _point(tmp_path, monkeypatch)
    path.write_text("not-a-timestamp\n", encoding="utf-8")
    bak = Path(f"{path}.bak")
    bak.write_text(f"{TS.strftime(ap.TS_FORMAT)}\n", encoding="utf-8")
    with caplog.at_level(logging.WARNING):
        got = ap.read_watermark()
    assert got == TS


def test_read_returns_none_when_both_missing(tmp_path, monkeypatch):
    _point(tmp_path, monkeypatch)
    assert ap.read_watermark() is None


def test_startup_retries_when_pg_connect_raises(monkeypatch):
    """create_db_and_tables fails twice then succeeds; watermark preserved."""
    calls = {"n": 0}
    sleeps: list[float] = []

    def boom_then_ok():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("postgres unreachable")

    monkeypatch.setattr(ap, "create_db_and_tables", boom_then_ok)
    monkeypatch.setattr(ap, "read_watermark", lambda: TS)
    monkeypatch.setattr(ap, "time", type("T", (), {
        "sleep": staticmethod(lambda s: sleeps.append(s)),
        "monotonic": staticmethod(lambda: 0.0),
    })())
    # Exit the main loop immediately after startup succeeds.
    monkeypatch.setattr(
        ap,
        "run_cycle",
        lambda wm: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    monkeypatch.setattr(ap, "write_watermark", lambda v: None)
    monkeypatch.setattr(ap, "drop_ch_client", lambda: None)
    monkeypatch.setattr(ap, "purge_health", lambda: 0)
    monkeypatch.setattr(ap, "ERROR_SLEEP_SEC", 12)
    monkeypatch.setattr(ap, "POSITION_LOOP_SLEEP_SEC", 0)
    monkeypatch.setattr(ap, "HEALTH_PURGE_INTERVAL_HOURS", 24)

    ap.main()

    assert calls["n"] == 3
    assert sleeps == [12, 12]
