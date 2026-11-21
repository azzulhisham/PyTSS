# aisposition.py watermark hardening

**Date:** 2026-11-04  
**Scope:** `ais-processor/aisposition.py` (Class A) + unit tests only  
**Not touched:** `aisposition_b.py`, ClickHouse queries, DDL, deploy, push  
**Prior report archived as:** [`reports/cursor-report-20261029-aisposition-sentinel-deploy.md`](cursor-report-20261029-aisposition-sentinel-deploy.md)

---

## 1. Problems addressed

| # | Issue | Fix |
|---|-------|-----|
| 1 | In-place overwrite of `pnav_aisposition.txt` can leave an empty file on kill/power loss | Atomic write: `.tmp` → `flush` + `os.fsync` → `os.replace` |
| 2 | `write_watermark(None)` wiped a good resume point on empty live cycles | Skip write when value is `None` (debug log) |
| 3 | No recovery if main file is empty/corrupt | Keep previous good value in `.bak`; `read_watermark` falls back with a warning |
| 4 | `create_db_and_tables()` at startup crashed the process if Postgres was unreachable | Retry with `ERROR_SLEEP_SEC`; read watermark from disk **before** PG so the resume point is never lost at boot |
| 5 | Startup failure exited the process | Only `KeyboardInterrupt` leaves `main()`; other startup errors retry |

Live/catch-up logic, `db_health` `msgCnt`, and ClickHouse/Postgres query shapes are unchanged.

---

## 2. Code changes

| Area | Detail |
|------|--------|
| `write_watermark` | Refuse `None`; write `.tmp` with fsync; refresh `.bak` from previous good main; `os.replace` |
| `read_watermark` | Parse main; on miss/empty/unparseable → try `.bak` and warn |
| Helpers | `_watermark_bak_path`, `_watermark_tmp_path`, `_parse_watermark_text`, `_read_watermark_file` |
| `main()` | `watermark = read_watermark()` first; loop `create_db_and_tables()` until success; cycle `except Exception` unchanged |

Files on disk (relative to `ais-processor/`):

| File | Role |
|------|------|
| `pnav_aisposition.txt` | Current resume cursor |
| `pnav_aisposition.txt.bak` | Last previous good value |
| `pnav_aisposition.txt.tmp` | Transient; removed via `replace` |

---

## 3. Tests

New: `ais-processor/test_aisposition_watermark.py` (no network).

| Test | Asserts |
|------|---------|
| `test_atomic_write_creates_main_and_bak` | Main written; `.tmp` gone; second write refreshes `.bak` |
| `test_none_is_not_written` | Existing file unchanged when value is `None` |
| `test_fallback_to_bak_when_main_missing` | Reads `.bak` + warning |
| `test_fallback_to_bak_when_main_empty` | Same |
| `test_fallback_to_bak_when_main_corrupt` | Same |
| `test_read_returns_none_when_both_missing` | `None` |
| `test_startup_retries_when_pg_connect_raises` | Two failures then success; sleeps `ERROR_SLEEP_SEC`; watermark from startup read preserved |

Also re-ran existing `test_position_gate.py`.

### Results

```
./venv/bin/python -m pytest -q test_aisposition_watermark.py test_position_gate.py
..................                                                       [100%]
18 passed in 1.40s
```

| Suite | Passed | Failed |
|-------|--------|--------|
| `test_aisposition_watermark.py` | 7 | 0 |
| `test_position_gate.py` | 11 | 0 |
| **Total** | **18** | **0** |

---

## 4. Risks / residual

| Risk | Mitigation / note |
|------|-------------------|
| `.bak` write itself is not fsync+replace | Acceptable: main path is atomic; bak is best-effort recovery |
| Seq of empty live cycles with `watermark is None` and no `.bak` | Still cannot invent a resume point (same as before); at least we no longer wipe a good file |
| Class B writer (`aisposition_b.py`) | Out of scope per prompt — same hardening not applied there yet |
| Not deployed | Local commit only; production still runs previous watermark code until an approved deploy |

---

## 5. Decision

| Item | Status |
|------|--------|
| Atomic write + no empty overwrite + `.bak` fallback | Done |
| Startup PG retry | Done |
| Tests | 18/18 pass |
| Push / deploy / DB | **Not done** (per instructions) |
