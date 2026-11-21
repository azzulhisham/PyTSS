# Empty ClickHouse window fix for TSS position catch-up

**Date:** 2026-11-21
**Scope:** `ais-processor/aisposition.py`, `ais-processor/aisposition_b.py`, new unit tests, `.env.example` keys only
**Not touched:** `position_gate.py`, static writers, analyzer, deploy manifests, watermark files, Dockerfiles, PyVAM, srv-01, git
**Prior report archived as:** [`reports/cursor-report-20261104-aisposition-watermark.md`](cursor-report-20261104-aisposition-watermark.md)

Both position scripts matched the deployed line counts before this edit (class A 960 lines, class B 748 lines). Live-mode query shape, catch-up query shape, upsert/dedupe, `LIVE_LOOKBACK_MINUTES` / `WINDOW_SECONDS` defaults, and watermark read/write are unchanged.

---

## 1. What broke

After the 21 Nov 2026 source gaps, catch-up queried a slice with no rows. ClickHouse returned an empty result with no `column_names`. `fetch_positions` raised `ValueError: 'ts' is not in list` before `run_cycle` could treat the window as zero rows and step the cursor. `main()` logged `Cycle failed`, slept, and retried the same window. The watermark file stayed put.

---

## 2. Files changed

| File | Change |
|------|--------|
| `ais-processor/aisposition.py` | Empty-result guard, window log, stall counters, optional watchdog |
| `ais-processor/aisposition_b.py` | Same behaviour, class B return shape kept separate |
| `ais-processor/test_aisposition_empty_window.py` | New offline tests for both modules |
| `ais-processor/.env.example` | `STALL_CYCLES=10`, `WATCHDOG_EXIT_FAILURES=0` |
| `reports/cursor-report.md` | This report |

---

## 3. What each change does

| Piece | Behaviour |
|-------|-----------|
| Empty ClickHouse result | After a successful `query()`, a missing result, empty `column_names`, missing `ts`, or no rows returns the caller's empty success value. Class A: `([], {}, 0, 0, 0)`. Class B: `([], 0, 0, 0)`. `run_cycle` then logs `No rows returned (mode=...)` and, in catch-up, returns `watermark + WINDOW_SECONDS`. Live + empty still returns the existing watermark. |
| Real ClickHouse error | Still logged, client dropped, exception re-raised. The cycle retries the same window. |
| Window log | One INFO line per cycle: `window mode=%s start=%s end=%s`. Catch-up bounds are the watermark and watermark + `WINDOW_SECONDS`. Live bounds are now − `LIVE_LOOKBACK_MINUTES` and now. No SQL, hosts, or row payloads. |
| Stall counters | Two streaks, both defaulting to a log at `STALL_CYCLES` (default 10), then again at each further multiple (20, 30, …). The `max(ts)` probe runs only on those cycles. |
| Failure streak | Incremented in `main()` on `Cycle failed`. Startup retries and health-purge errors stay outside this counter. |
| Empty catch-up streak | Incremented only when catch-up returns 0 rows and the watermark does not move forward. A healthy gap walk (`watermark + WINDOW_SECONDS`) clears the streaks and does not log `STALL`. |
| `STALL` line | WARNING, token `STALL`, plus kind, mode, count, watermark, and `ch_max_ts` when `max(ts)` is inside `LIVE_LOOKBACK_MINUTES`. A stale `max(ts)` stays quiet. If the probe itself fails, the line is still written with `freshness=check_failed` and the cycle continues. |
| Progress reset | A successful cycle clears both streaks: live with rows (including `inserted=0`), catch-up that steps forward, catch-up that returns rows, and live empty. A frozen empty catch-up clears only the failure streak and increments the stuck streak. |
| Watchdog | `WATCHDOG_EXIT_FAILURES` default `0`. When set to M > 0, `SystemExit(1)` after M consecutive cycle exceptions so systemd `Restart=on-failure` can restart the unit. Empty windows and `inserted=0` live cycles do not count. |

`SELECT max(ts) FROM <existing CH table>` is a separate probe. `build_query` is unchanged.

---

## 4. Tests

New file: `ais-processor/test_aisposition_empty_window.py` (no network). Each case runs for class A and class B.

| Test | Asserts |
|------|---------|
| `test_empty_catchup_does_not_raise_and_advances` | No columns, `column_names=None`, no `ts`, or columns with no rows: catch-up advances by `WINDOW_SECONDS`, logs the window, does not log `STALL` |
| `test_fetch_empty_returns_caller_shape` | Class A `([], {}, 0, 0, 0)`, class B `([], 0, 0, 0)` |
| `test_query_error_still_raises_and_drops_client` | Connection error propagates and `drop_ch_client` runs |
| `test_gap_then_data_lands_on_first_row_ts` | Three empty catch-up steps, then one row; watermark ends on that row's ts |
| `test_advancing_empty_catchup_does_not_stall` | Five forward empty steps, cursor moves, probe is not called, no `STALL` |
| `test_stuck_empty_catchup_logs_stall_then_progress_resets` | Cursor frozen for N cycles with a recent `max(ts)` logs one `STALL`; a later row clears the counters; the next short freeze does not log again |
| `test_stuck_stall_skipped_when_source_is_stale` | Frozen cursor and an old `max(ts)`: counter climbs, no `STALL` |
| `test_stuck_stall_when_freshness_probe_fails` | Probe failure still logs `STALL` with `freshness=check_failed` and the cycle returns |
| `test_live_inserted_zero_does_not_stall_and_resets` | Live cycle, rows > 0, `inserted=0`: counters cleared, no `STALL`, `mode=live` logged |
| `test_consecutive_failures_log_stall` | N exceptions in `main()` log one `STALL kind=failure` |
| `test_watchdog_exits_after_consecutive_failures` | `WATCHDOG_EXIT_FAILURES=2` raises `SystemExit(1)` after two failures |

Also re-ran `test_aisposition_watermark.py` and `test_position_gate.py`.

### Results

```
./venv/bin/python -m pytest -q test_aisposition_empty_window.py test_aisposition_watermark.py test_position_gate.py
..............................................                           [100%]
46 passed in 1.96s
```

| Suite | Passed | Failed |
|-------|--------|--------|
| `test_aisposition_empty_window.py` | 28 | 0 |
| `test_aisposition_watermark.py` | 7 | 0 |
| `test_position_gate.py` | 11 | 0 |
| **Total** | **46** | **0** |

---

## 5. Residual risks

| Risk | Note |
|------|------|
| Healthy gap walk stays quiet | Empty catch-up that steps forward does not log `STALL`, even when ClickHouse `max(ts)` is recent. `STALL` on that path means the cursor did not move. |
| Freshness window is the live lookback | `max(ts)` counts as recent only when it falls inside `LIVE_LOOKBACK_MINUTES`. A class A unit whose lookback is 2 minutes needs `max(ts)` inside those 2 minutes. A failed probe still logs `STALL`. |
| Probe cost | `SELECT max(ts)` runs when a streak hits 10, 20, 30, … cycles, using the existing ClickHouse timeouts. It can add one connect attempt on a dead client at that moment. |
| Watchdog default off | A failed cycle still logs, sleeps `ERROR_SLEEP_SEC`, and continues until an operator sets `WATCHDOG_EXIT_FAILURES`. |
| Log only | There is no telegram, mail, or webhook. `STALL` is a journal line. |
| Live window clock | The INFO line uses the process UTC clock. The live SQL still uses ClickHouse `now()`, so the logged end can differ from the server clock. |
| Not deployed | srv-01 still runs the previous files until a human copies them and restarts the units. |

---

## 6. Later check on srv-01

After a human deploy, not as part of this change:

```text
journalctl -u ais_tss_aisposition.service --since "10 minutes ago" | grep -E "ValueError|STALL|catch-up|No rows returned|mode=live|Cycle failed"
journalctl -u ais_tss_aisposition_b.service --since "10 minutes ago" | grep -E "ValueError|STALL|catch-up|No rows returned|mode=live|Cycle failed"
```

Normal live operation: `window mode=live` lines, class A on the order of ~2,240 rows per 2-minute window, class B on the order of ~1,380–1,420 rows per its lookback, watermarks moving, no `ValueError: 'ts' is not in list`, no `STALL`.

If the source gaps again: INFO window bounds, `No rows returned (mode=catch-up)`, watermark stepping by `WINDOW_SECONDS` (600 seconds) through the empty slice, then `mode=live` once the cursor is inside the live lookback, without `Cycle failed` or `ValueError`. `STALL` appears when the cursor is still frozen, or failures keep repeating, and ClickHouse `max(ts)` is recent or the freshness probe fails.

---

## 7. Decision

| Item | Status |
|------|--------|
| Empty result returns empty success and catch-up can advance | Done |
| Window bounds logged each cycle | Done |
| `STALL` log, default 10 cycles, probe only when logging | Done |
| Watchdog exit, default off | Done |
| Tests | 46/46 pass |
| Commit / push / deploy / srv-01 | Not done |
