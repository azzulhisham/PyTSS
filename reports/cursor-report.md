# AIS position writer: keep reports whose course is not available

29 Oct 2026. The service has not been restarted. Deploy is waiting for go-ahead because this writer updates pnav.

PyTSS had no previous `reports/cursor-report.md`.

## Choice

COG and SOG stay as the AIS not-available numbers: **COG 360** and **SOG 102.2**.

`public.ais_position.cog` and `public.ais_position.sog` are `double precision NOT NULL`. The same is true on `public.ais_positionb` (`trueHeading`, latitude, and longitude are NOT NULL on both tables). A NULL would fail the batched UPDATE/INSERT, and the task forbids DDL. A missing course or speed is filled with 360 or 102.2 so the column can be written. A value that is already at or above the sentinel is stored unchanged.

`trueHeading` 511 is unchanged. That column is also NOT NULL, and no reader needed a different value.

## Cause

`ais_tss_aisposition.service` runs `aisposition.py` and copies the newest ClickHouse `pnav.ais_position` rows into `public.ais_position` about every 2.5 s.

`_is_itu_sentinel()` treated `sog >= 102.2` or `cog >= 360` as a reason to drop the whole row, before the rows were grouped per MMSI. The position never reached the UPDATE. In AIS, COG 360 means course not available. That is normal for a ship that is not moving.

OCEAN PRIDE (MMSI 677068800) had valid reports after 06:16 MYT, all with COG 360, so its Postgres row stayed at 06:16.

The UPDATE predicate (`t.ts <= v.ts`, `IS DISTINCT FROM`) was already correct. The same drop was in the 7 Oct and 22 Oct backups.

Read-only finding, 29 Oct about 17:50 MYT:

| Measure | Value |
|---|---|
| Rows dropped per cycle, logged as `itu=` | about 1,120 of about 2,780 |
| Active MMSIs whose every report was COG 360 | 199 of 3,242 |
| Postgres rows older than 1 hour | 167 |
| Postgres rows older than 24 hours | 58 |
| Typical ships | berthed and anchored |

## Changes

| Item | What it does now |
|---|---|
| Position | A row is skipped only when latitude or longitude is missing or outside [-90, 90] / [-180, 180]. |
| COG >= 360 | Row is kept. The number is stored. Logged as `field_sentinel`. |
| SOG >= 102.2 | Row is kept. The number is stored. Logged as `field_sentinel`. |
| Missing COG or SOG | Filled with 360 or 102.2, because the column is NOT NULL. Logged as `field_sentinel`. |
| trueHeading 511 | Left as it arrives. |
| Other missing NOT NULL fields (nav status, description, rot, true heading) | Still skipped, as `skipped_null`, so one empty field cannot fail the batch. |
| Stationary throttle | In-memory last write for this process. No extra pnav SELECT. |
| Cycle, lookback, row limit | Unchanged: about 2.5 s, 10-minute lookback, 5 ClickHouse rows per MMSI. |

Stationary rule, using only memory filled from rows this process has written:

- Within `STATIONARY_MOVE_M` (default 10 m), same nav status, and speed under `STATIONARY_SOG_KN` (default 0.5 kn): write only when the remembered timestamp is at least `STATIONARY_REFRESH_SEC` (default 120) older than the new report.
- A move beyond 10 m, a nav-status change, speed at or above 0.5 kn, or a ship that has just stopped, is written on that cycle.
- Memory is empty after a restart, so the first report is written (this is what unfreezes OCEAN PRIDE) and later repeats wait.
- A throttled row is not remembered, so the 120 s clock starts at the last real write.
- Inserts and teleport rewinds are not throttled.
- The watermark still follows the newest timestamp seen, including throttled rows.

The three limits are environment variables, with defaults in `_DEFAULTS`, same pattern as the other settings. No new server paths.

Log line (replaces `itu=`):

```text
invalid=%s field_sentinel=%s stationary_throttled=%s
```

`invalid` is the position drop. `field_sentinel` is a row that was kept with a course or speed sentinel. `stationary_throttled` is a write skipped from memory.

### Class B

`aisposition_b.py` did not have `_is_itu_sentinel`, so it was not dropping COG 360. `ais_positionb.cog` and `.sog` are also NOT NULL, so the same storage rule applies. The file now uses the same position gate and the same in-memory throttle. Its stationary repeats were rewriting pnav on every fresh timestamp. After approval, restart the class B unit as well.

Shared logic is `ais-processor/position_gate.py`. The live folder is a flat script directory, so that file is deployed beside the two writers.

## Downstream readers

Stored values stay numbers, so none of these readers break. NULL was the path that would have failed the writer and some inserts. That path is not used.

| Reader | How it uses cog / sog | With COG 360 and SOG 102.2 stored |
|---|---|---|
| PyVAM `ship_shape.heading_from` | true heading 0–359 wins. COG is used only when SOG >= 1 kn, and COG 360 is drawn as 0. Slow ships ignore COG. | Safe. A moored ship does not turn from its COG. |
| PyVAM `00_ship_layers.js` | Same rule, including `cog === 360`. | Safe. |
| PyVAM port metrics, port logic, geo, delay job | `float(sog or 0)`. | Safe. |
| PyVAM `track_arrivals.py` | `float(sog or 0)`. COG is copied onto the visit. | Safe. 360 is a number. |
| PyVAM `port_view.py` / `map_figure.py` | Heading comes from `heading_from`. | Safe. |
| PyVAM `port_map.py` | A missing COG is drawn as 0. A present COG is the marker angle. | Safe. 360 draws as north, which matches the heading rule for an unavailable course. |
| PyVAM `anchorage_wait.py` | Reads `trueHeading` and `cog` by MMSI. 511 is not a heading. | Safe. 511 is still stored as 511. |
| MANTIS via PySTS `identity_conflict.py` | Selects `p.cog` and `p.sog`. `float` only after a null check. | Safe. |
| MANTIS via PySTS `sts_detection.py` | COG and SOG come from the activity row, with `pd.notna` before `float`. The position join is for time. | Safe. |
| PySTS `spoofing.py`, `timelineplayback.py` | Read ClickHouse `pnav.ais_position`, not this Postgres row. | Unaffected. |
| PySTS `vesselzone.py` | Copies `cog` and `sog` into zone rows typed as non-optional floats. | Safe. 360 and 102.2 are floats. |
| PySTS `vesselzone_b.py` | Same, from `ais_positionb`. | Safe. |
| PySTS `vesselslowspeeddetection.py` | SQL filters on SOG. `estimate_latlng` uses `math.radians(cog)` and has no caller. | Safe. |
| PySTS `vesselstrajectorydetection.py` | Copies `cog` and `sog` onto the activity. New-activity columns are non-optional floats. | Safe. |
| PyTSS `analyzer/vesselzone.py` | Reads `ais_position` and copies `cog` and `sog`. The position model requires a float. | Safe. |
| PyTSS-Reporting SQL | `WHEN vz.cog IS NULL THEN vp.cog`. | Safe. The position value is a number, so the comparison still works. |

No reader needed a stop.

## Tests

`ais-processor/test_position_gate.py`, 11 passed. No database.

| Test | Result |
|---|---|
| Moored row, COG 360, SOG 0 | Kept. Position and timestamp unchanged. COG stored as 360. Heading 511 unchanged. |
| Latitude 91, longitude 181 | Skipped as an invalid position. |
| COG 40, SOG 8.2 | Returned unchanged, and not counted as a sentinel. |
| Missing COG | Filled with 360. |
| SOG 102.2 | Kept as 102.2. |
| Stationary reports every 3 minutes | Each one is written. |
| Stationary reports every 10 seconds | Written at 0, 120, and 240 seconds only. |
| Move of about 22 m, nav-status change, or SOG 0.6 | Written on that cycle. |
| Ship that was at 8 kn and is now stopped | Written on that cycle. |
| Writer sources | `_is_itu_sentinel` is gone. Both scripts log `field_sentinel` and `stationary_throttled`. |

## Before and after

| Counter | Before (29 Oct, about 17:50 MYT) | After |
|---|---|---|
| `itu=` drops per cycle | about 1,120 of about 2,780 | Log key removed. Not measured live. |
| `invalid=` | not split out | Pending the approved restart. Expect a small number (bad coordinates only). |
| `field_sentinel=` | included in `itu=` | Pending. Expect on the order of the old `itu=` count, and those rows are kept. |
| `stationary_throttled=` | 0 (every fresh timestamp was eligible) | Pending. Expect this to rise after the first cycle, with cycle time still near 2.5 s. |

## Stale rows

Not recounted. The service is still the old process.

| MMSI | Expected after restart |
|---|---|
| 677068800 OCEAN PRIDE | First cycle writes the current fix. COG stays 360. Later reports while it remains within 10 m and under 0.5 kn refresh every 120 s. |
| 167 rows older than 1 hour | Pending a read-only recount by MMSI after the journal window. |
| 58 rows older than 24 hours | Same recount. |

## Deploy

Not done. Waiting for go-ahead.

When approved:

1. Back up `/home/srvadmin/py/tss/aisposition/` first.
2. Copy `aisposition.py`, `aisposition_b.py`, and `position_gate.py`. The live folder is not a git checkout.
3. Check sha256 of those three files against this commit.
4. Restart only `ais_tss_aisposition`. Restart the class B unit as well (`ais_tss_aisposition_b`, confirm the unit name on the host) because that file changed.
5. Watch 15 minutes of journal. `invalid` should stay small, there should be no tracebacks, and cycle time should stay near 2.5 s. `field_sentinel` replaces the old `itu` drops. `stationary_throttled` should be greater than zero after the first cycle.
6. Read OCEAN PRIDE with `WHERE mmsi = 677068800` only. Confirm `ts` is current and `cog` is 360.
7. Recount how many of the 167 stale MMSIs are current. Same style of read: key by MMSI, statement timeout 30 s, lock timeout 5 s, read only.

## Commits

| Commit | Subject |
|---|---|
| `5c5685c6997f438ee0986a3ba62934e5dd15777e` | Keep AIS position reports when course or speed is not available. |

Not pushed. The parent on main was `2c95cf5`.
