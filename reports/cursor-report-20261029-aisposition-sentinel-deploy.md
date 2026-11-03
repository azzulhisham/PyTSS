# AIS position writer deploy

29 Oct 2026. Both writers were restarted on srv-01 at 11:01:54 UTC. No rollback.

The pre-deploy write-up is `reports/aisposition-sentinel.md`.

## Result

Valid positions with COG 360 are written. OCEAN PRIDE (MMSI 677068800) moved from a row frozen at 06:16 MYT to the latest ClickHouse report, with COG stored as 360.

| | Before | After (15 min from 11:01:54 UTC) |
|---|---|---|
| Class A cycle | 2.54 s | 2.42 s |
| Class A `itu=` per cycle | 1,087 | counter removed |
| Class A `invalid` per cycle | (inside `itu`) | 82 |
| Class A `field_sentinel` per cycle | (inside `itu`) | 1,004 |
| Class A `stationary_throttled` | 0 | 0 on the first cycle, then at least 1,907 |
| Class A rows offered to UPDATE per min | 64,934 | 24,493 |
| Class A row versions (`changed`) per min | 5,906 | 4,566 |
| Class B cycle | 2.24 s | 2.20 s |
| Class B `invalid` / `field_sentinel` / throttled | no `itu` counter | 22 / 129 / 802 after the first cycle |
| Class B rows offered to UPDATE per min | 35,214 | 13,345 |
| Class B row versions per min | 661 | 565 |
| Tracebacks | 0 | 0 |
| Slow-query lines in either journal | 0 | 0 |

`invalid` plus `field_sentinel` is 1,086, the same size as the old `itu` drop. The course and speed sentinels are the rows now kept. What remains in `invalid` is a bad position.

`log_min_duration_statement` is `-1` (duration logging is off). At 11:25 UTC, `pg_stat_activity` had no other query running longer than 1 second. The check used `statement_timeout` 30 s, `lock_timeout` 5 s, and a read-only transaction.

## OCEAN PRIDE

Keyed `WHERE mmsi = 677068800`.

| | Postgres `ts` (UTC) | cog | sog |
|---|---|---|---|
| Before restart | 2026-10-28 22:16:47 (06:16 MYT) | 96.9 | 0.4 |
| After, at 11:25 UTC | 2026-10-29 11:22:42 | 360 | 0.0 |

The latest ClickHouse report for this MMSI is the same timestamp, COG 360, SOG 0. The ship reports about every 6 minutes, so the 120 s stationary hold does not delay it.

## Stale rows

The 17:50 MYT finding was 199 MMSIs with every report at COG 360, 167 of them with a Postgres row older than 1 hour and 58 older than 24 hours. That MMSI list was not saved. Immediately before the restart, the same definition was taken again and saved: every ClickHouse report in the previous hour had COG >= 360.

| Cohort, saved at 11:00 UTC | Ships | Postgres `ts` at or after 11:01:54 UTC | Still old |
|---|---|---|---|
| Older than 1 hour | 203 | 174 | 29 |
| Older than 24 hours | 78 | 58 | 20 |

The 29 that are still old:

| Reason | Ships |
|---|---|
| No ClickHouse report after the restart | 12 |
| Every new report is latitude 91 and longitude 181 | 17 |

Those 17 are the position-not-available sentinel. Spot checks of 511101401, 533167000, and 352003432 are latitude 91, longitude 181, COG 360. The writer still skips that position. All 20 of the still-old 24-hour ships are inside those 29.

## Cause

`_is_itu_sentinel()` dropped a whole row when SOG was at least 102.2 or COG was at least 360, before the per-MMSI group. A ship that only reports COG 360 never reached the UPDATE. The UPDATE predicate itself was already right.

## What was deployed

Commit `5c5685c6997f438ee0986a3ba62934e5dd15777e`. The python files are unchanged after that commit (`f6c921d` and `ad4371c` are the report only).

| Rule | Behaviour |
|---|---|
| Position | Skip only a missing latitude or longitude, or a value outside [-90, 90] / [-180, 180]. |
| COG >= 360 and SOG >= 102.2 | Keep the row. Store the number. Both columns are `NOT NULL`, so the value is not stored as NULL. |
| Missing COG or SOG | Fill 360 or 102.2 for the same reason. |
| trueHeading 511 | Unchanged. |
| Stationary hold | Memory of the last row this process wrote. Within 10 m, same nav status, speed under 0.5 kn: write only when that memory is at least 120 s older. No extra pnav read. |
| Limits | `STATIONARY_REFRESH_SEC`, `STATIONARY_MOVE_M`, `STATIONARY_SOG_KN`. Cycle sleep, 10-minute lookback, and 5 rows per MMSI are unchanged. |

Class B did not drop COG 360. `ais_positionb.cog` and `.sog` are also `NOT NULL`, and a stationary class B ship was rewritten on every fresh timestamp. It now uses the same gate and the same hold.

## Downstream readers

Stored COG and SOG stay numbers. No reader needed a stop.

| Reader | With COG 360 stored |
|---|---|
| PyVAM `heading_from` and `00_ship_layers.js` | COG is used only at SOG >= 1 kn, and 360 is drawn as 0. A moored ship keeps its heading. |
| PyVAM port metrics, arrivals, port map | Speed uses `sog or 0`. A present COG of 360 is a number. |
| MANTIS via PySTS identity and STS | `float` only after a null check. |
| PySTS spoofing and timeline playback | Read ClickHouse, not this Postgres row. |
| PySTS and PyTSS vessel zone, slow speed, trajectory | Copy `cog` and `sog` into float columns. |

## Tests

`ais-processor/test_position_gate.py`, 11 passed before the copy. A moored COG 360 row is kept with COG 360. Latitude 91 and longitude 181 are skipped. A normal row is unchanged. A stationary ship every 3 minutes is written every time. One reporting every 10 seconds is written at 0, 120, and 240 seconds.

## Deploy steps

| Step | Result |
|---|---|
| Backup | `/home/srvadmin/py/tss/aisposition-backup-20261029-110134` (timestamped copy of that folder only) |
| Files copied | `aisposition.py`, `aisposition_b.py`, `position_gate.py` |
| sha256 | all three live files match the commit (below) |
| Class B unit | `ais_tss_aisposition_b.service` |
| Restart | only `ais_tss_aisposition.service` and `ais_tss_aisposition_b.service`, at 2026-10-29 11:01:54 UTC |
| PIDs | class A 1296170 → 2188080. Class B 1523778 → 2188081. `NRestarts=0` through 11:25 UTC |
| Journal | 11:01:54–11:16:54 UTC. No traceback. Cycle time near 2.5 s. `stationary_throttled` is 0 on cycle 1 and then above 1,900 (class A) and 770 (class B) |
| Rollback | not used |

| File | sha256 |
|---|---|
| `aisposition.py` | `7dbee038dc86c611c33b0824868498eeb492cb7e62e97bfb15a41d606bd6c4b2` |
| `aisposition_b.py` | `bcdec98758a438456d590a3f8acacb6953498787863c8170a9c2d6c74eb8d0e3` |
| `position_gate.py` | `c409d97cb0df3a288c34f695065de591a90973c2d86f214c80d4c298c62e0aa1` |

## Commits

| Commit | Subject |
|---|---|
| `5c5685c6997f438ee0986a3ba62934e5dd15777e` | Keep AIS position reports when course or speed is not available. |
| `f6c921d401d8f14b644f9e35862be9f18e18f8ac` | Record the AIS position-sentinel report. |
| `ad4371ccad2a06702b83c2fe543f18086478e897` | Fill in the position-sentinel report hash. |

The python files on the host are the tree at `5c5685c`. Not pushed.
