# Install all requirement libraries using pip
pip install -r requirement.txt



# -----  Data Analyzer  -----
# run the flask application using the following command
python3 ./analyzer/vesselzone.py

# to build docker image
docker build --platform linux/amd64 -t azzulhisham/py-tss-analyzer-linux:v1.20 -f Dockerfile_analyzer .  

# push image to docker hub
docker push azzulhisham/py-tss-analyzer-linux:v1.20

# deploy to kubernetes
kubectl apply -f deployment_analyzer.yaml -n system-pnav


---

# Vessel Zone Analyzer — Maintenance Guide

This document describes `vesselzone.py` for future maintenance. The script watches AIS vessel positions and records when ships enter, stay in, or leave defined sea zones.

## Purpose

Continuous service that:

1. Reads the latest vessel position from `public.ais_position` (joined with `public.ais_static`).
2. Checks which zone polygon(s) the vessel is inside.
3. Upserts results into `public.ais_vesselinzone`:
   - **Enter zone** → `INSERT` (`tsOut` is `NULL`)
   - **Still inside** → `UPDATE` current position / time
   - **Leave zone** → `UPDATE` and set `tsOut`
4. Periodically closes stale open records that are no longer in the TSS area.

Related upstream job: `PyTSS/ais-processor/aisposition.py` updates `public.ais_position` about every minute (one row per MMSI).

## Main files

| File | Role |
|------|------|
| `vesselzone.py` | Main loop, DB I/O, zone membership, upsert |
| `polygons.py` | Zone GeoJSON / coordinate definitions |
| `vesselzone_watermark.txt` | Local watermark (created at runtime; last processed `ts`) |
| `requirements.txt` / `requirement.txt` | Python dependencies |

## How one cycle works

```text
[START]
  → init DB tables + load zone polygons into DuckDB (once)
loop:
  1. FETCH   latest position per MMSI (incremental or 1-day bootstrap)
  2. UPSERT  batch spatial join → enter / stay / leave → write ais_vesselinzone
  3. CLEANUP close stale open-zone rows (>15 days and outside TSS)
  4. SLEEP   2 seconds
```

Logs are written to stdout (visible via `systemctl status` / `journalctl`), for example:

```text
[CYCLE N] stage=fetch|upsert|cleanup
[CYCLE N] OK :: rows=..., fetch=...s, upsert=...s, cleanup=...s, total=...s
[CYCLE N] FAILED at stage=... :: <error>
```

## Data extract rules

Goal: process **latest 1 row per MMSI**, with as few rows as possible.

| Condition | Behaviour |
|-----------|-----------|
| Watermark file **missing** | Load last **1 DAY**, `DISTINCT ON (mmsi)` ordered by `ts DESC` |
| Watermark file **exists** | Load only rows with `ts > last_run_ts`, still latest 1 row per MMSI |
| Always | SQL bbox around padded TSS region (from `get_entire_tss_region_setting()`) |

Watermark path (local, next to the script):

```text
analyzer/vesselzone_watermark.txt
```

Format: `YYYY-MM-DD HH:MM:SS`

- Updated only after a successful upsert that processed data.
- Delete this file to force a full **1-day** re-bootstrap on next start.
- Ships with **no new position** since last run are **skipped** until they report again.

## Zone IDs (important)

Zone id stored in `ais_vesselinzone.zone` is the **index in the `zones` list** in `vesselzone.py`.

| Index | Zone |
|------:|------|
| 0 | restrictedlimit |
| 1–9 | sector1 … sector9 |
| 10 | TSS Northbound |
| 11 | TSS Southbound |
| 12–20 | `None` placeholders (not used; keep positions so later ids stay stable) |
| 21–26 | sector101 … sector106 |

**Do not reorder or remove `None` slots** without a data migration. Changing list order changes zone ids in the database.

`None` entries are skipped during spatial checks, but they still reserve index numbers (so sector101 stays **21**).

## Enter / stay / leave behaviour

For each vessel in the current batch:

1. DuckDB runs **one** spatial join: points × all zone polygons.
2. Compare result with open rows (`tsOut IS NULL`) keyed by `(mmsi, zone)`.

| Situation | Action |
|-----------|--------|
| Inside zone, no open row | INSERT |
| Inside zone, open row exists | UPDATE (`tsCurrent`, current lat/lon, fill empty static fields if needed) |
| Not inside zone, open row exists | UPDATE set `tsOut` to vessel position time |
| Open row, still inside, age > 15 days, and `zone <= 11` | Force set `tsOut` (same rule as legacy script) |

Notes:

- Sectors **101–106** (ids 21–26) **do** get `tsOut` on real exit.
- The **15-day force-close while still inside** applies only to `zone <= 11`, not to 101–106.
- Cleanup (`chk_invalid_data`) closes open rows older than 15 days when the vessel’s current position is outside the whole TSS region.

## Performance design (why it is fast)

Older approach: for every ship × every zone, run a separate DuckDB `ST_Within` call (very slow).

Current approach:

1. Parse all zone geometries **once** at startup into DuckDB table `zone_geoms`.
2. Each cycle: **one** batch spatial join.
3. Incremental fetch via watermark (few rows per cycle in steady state).
4. Dict lookup for open `(mmsi, zone)` records.
5. DuckDB limited to **2 threads** and **1GB** memory to avoid overloading the Linux host.
6. Shared SQLAlchemy engine; summary logging; batched DB writes.

Typical steady-state cycle (example from production): fetch ~0.05s, upsert ~0.5s, cleanup ~0.2s, total under **1s**, then sleep 2s.

Largest remaining cost in upsert is often loading many open `ais_vesselinzone` rows (`tsOut IS NULL`), not the spatial join.

## Dependencies / runtime

- Python 3 with packages: `sqlmodel`, `sqlalchemy`, `pandas`, `duckdb` (+ spatial extension), `psycopg2`, etc.
- DuckDB spatial extension is installed/loaded at process start.
- Target DB: PostgreSQL `pnav` (tables `ais_position`, `ais_static`, `ais_vesselinzone`).
- Intended to run as a long-lived Linux service (e.g. systemd).

## Operations cheat sheet

| Task | Action |
|------|--------|
| Install deps | `pip install -r requirement.txt` (from project docs above) |
| Run locally | `python3 ./analyzer/vesselzone.py` |
| Check health | `systemctl status <service>` / journal logs for `[CYCLE …] OK` or `FAILED at stage=` |
| Force full reprocess (1 day) | Stop service → delete `vesselzone_watermark.txt` → start service |
| Add a new zone | Append polygon in `polygons.py`, add to `zones` list **at the correct index** (prefer append; avoid shifting existing ids) |
| Change zone geometry only | Edit polygon in `polygons.py`; restart service so `zone_geoms` reloads |

## Troubleshooting

| Symptom | Likely cause | What to check |
|---------|--------------|---------------|
| `FAILED at stage=fetch` | DB connectivity / SQL / credentials | Postgres reachability, `ais_position` / `ais_static` |
| `FAILED at stage=upsert` | Write/spatial/type issues | Open-zone query, DuckDB spatial, NaN/int cleaning |
| `FAILED at stage=cleanup` | Stale-close query | `chk_invalid_data` SQL / DuckDB TSS check |
| Always processing huge row counts | Watermark missing or not writable | File path permissions; watermark update logs |
| Zone ids look “wrong” after edit | List order changed | Compare `zones` indexes with historical `ais_vesselinzone.zone` |
| Ship left zone but `tsOut` still null | No new AIS update yet, or outside SQL bbox | Wait for next position update; check watermark / bbox; 15-day cleanup |

## Safe change guidelines

1. Prefer **appending** new zones; do not renumber existing ones without migrating DB data.
2. Keep enter / stay / leave semantics unless product rules change.
3. Keep DuckDB thread/memory limits modest on shared Linux hosts.
4. After performance or logic changes, confirm with cycle timing logs (`fetch` / `upsert` / `cleanup` / `total`).
