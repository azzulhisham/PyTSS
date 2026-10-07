# Postgres health benchmark

Recorded **2026-10-07** after the writer changes to `aisposition`, `aisstatic`, `aisstatic_b`, `vesselzone` (TSS analyzer), `vesselzone_b` (PySTS), and the four STS detectors (loiter, proximity, slow-move, trajectory).

This is the “green” contract for `pnav` on RDS. Do not use instance class as the health signal. The instance was enlarged earlier to absorb sequential scans and no-op `UPDATE`s; those are gone. Keep the current size for headroom. If these numbers drift, fix the writer, not the hardware.

Health endpoint: `GET /tss/health/postgres` in PyTSS-Reporting (`app/main.py`, `get_postgres_health()`).

---

## Instance snapshot (2026-10-07)

| | value |
|---|---|
| Engine | PostgreSQL 17.9 (RDS, `aarch64`) |
| Estimated RAM | ~16 GB (`shared_buffers` 4.04 GB ≈ 25%) |
| `max_connections` | 1695 |
| `max_parallel_workers` | 8 |
| Database `pnav` | 2.8 GB |
| All databases | 2.9 GB |
| RDS uptime at measurement | ~6.7 days |

`client_addr` for `srv-01` (10.10.20.201) and `srv-dp` (10.10.20.200) both appear as `60.54.119.41` (office NAT). Identify sessions by `application_name`, not IP.

Named writers after today:

| `application_name` | host | script |
|---|---|---|
| `aisposition` / `aisposition_b` | srv-01 | `PyTSS/ais-processor` |
| `aisstatic` / `aisstatic_b` | srv-01 | `PyTSS/ais-processor` |
| `vesselzone` | srv-01 | `PyTSS/analyzer/vesselzone.py` |
| `sts_vesselzone_b` | srv-01 | `PySTS/backend/vesselzone_b.py` |
| `sts_trajectory` | srv-dp | `vesselstrajectorydetection.py` |
| `sts_slowspeed` | srv-dp | `vesselslowspeeddetection.py` |
| `sts_proximity` | srv-dp | `vesselproximitydetection.py` |
| `sts_loiter` | srv-dp | `vesselloiteringdetection.py` |

A **blank** `application_name` holding a transaction is the next script to fix.

---

## Healthy-state contract

Aligns with `/tss/health/postgres` plus the two signals that actually predicted pain today (WAL and sequential scans).

| signal | healthy | investigate | how we got `degraded` |
|---|---|---|---|
| Probe latency | &lt; 100 ms | 100–500 ms | not the cause today |
| Connections used | &lt; 70% of `max_connections` | 70–90% | we sit at ~1.4% |
| Max active query age | &lt; 60 s | 60–300 s | — |
| **Idle in transaction** | **&lt; 15 s** | 15–60 s | **&gt; 60 s = `degraded`** |
| WAL | **&lt; 15 GB/day** | 15–30 GB/day | 45.5 GB/day before the position fix |
| Seq rows/min on `ais_vesselinzone`, `ais_vesselmovementactivities`, `ais_static`, `ais_staticb` | **~0** | millions | 28 M + 4.2 M this morning |
| Writers in `pg_stat_activity` | named (`sts_*`, `vesselzone`, `aisposition*`, `aisstatic*`) | blank `application_name` holding a xact | old trajectory / slowspeed |

Watch idle-in-transaction **by `application_name`**. A 6 s `sts_slowspeed` session is normal. A blank session climbing through 20 s, 30 s, 40 s is not.

---

## Measured before → after (2026-10-07)

Approximate; WAL “before” is reconstructed from cumulative counters vs the position deploy. Sequential-scan and idle-in-transaction figures are live 60–90 s samples.

| metric | before today’s writers | after |
|---|---|---|
| Cluster WAL | 45.5 GB/day | 7.2 GB/day after position (−84%); static / static_b / trajectory took more off that |
| `ais_position` updates | 1,006/s (class A), 536/s (class B) | 126/s and 14/s |
| `ais_static` writes | 35.0/s | 3.4/s (−90%) |
| `ais_staticb` upsert | 157 s, 8,320 updates/cycle | 4–6 s, ~950 updates/cycle |
| `ais_vesselinzone` seq scans | 90/min, 28.2 M rows/min | 0 from the analyzer |
| `ais_vesselmovementactivities` seq scans | 13/min, 4.2 M rows/min | 0 |
| Worst idle in transaction | 15–36 s and climbing (blank name) | ~6 s, named |
| Probe latency | 7–40 ms | 8–46 ms (unchanged; was never the trigger) |
| Connections | ~19 / 1695 (1.1%) | ~23 / 1695 (1.4%) |
| Health samples | intermittent `degraded` | 10/10 and 12/12 healthy in post-deploy windows |

Table shrinks (fillfactor 70 + `VACUUM FULL`, locks held well under 1 s):

| table | heap before | heap after |
|---|---|---|
| `ais_position` | 101 MB | 9.8 MB |
| `ais_positionb` | 39 MB | 4.1 MB |
| `ais_static` | 70 MB | 8.4 MB |
| `ais_staticb` | 54 MB | 4.5 MB |

New partial indexes (all `CREATE INDEX CONCURRENTLY`):

- `ais_vesselinzone`: `idx_vesselinzone_open` on `(mmsi, "tsDetected" DESC) WHERE "tsOut" IS NULL` (136 kB)
- `ais_vesselinrestrictzone`: `idx_vesselinrestrictzone_open` (same shape)
- `ais_vesselmovementactivities`: `ix_ais_vesselmovementactivities_open` on `(mmsi, ts DESC) WHERE tsout IS NULL` (184 kB) and `ix_ais_vesselmovementactivities_open_ts` on `(ts) WHERE tsout IS NULL` (136 kB)

---

## How to re-measure

Idle in transaction (must stay under 60 s; investigate above 15 s):

```sql
SELECT coalesce(nullif(application_name, ''), '(blank)') AS app,
       client_addr,
       round(EXTRACT(epoch FROM now() - xact_start)::numeric, 1) AS xact_s,
       left(regexp_replace(query, '\s+', ' ', 'g'), 80) AS query
FROM pg_stat_activity
WHERE state = 'idle in transaction'
ORDER BY xact_start;
```

Sequential scan rate (sample, wait 60 s, sample again, subtract):

```sql
SELECT relname, seq_scan, seq_tup_read, n_tup_upd, n_tup_ins
FROM pg_stat_user_tables
WHERE relname IN (
  'ais_vesselinzone', 'ais_vesselmovementactivities',
  'ais_vesselslowmoveactivities', 'ais_static', 'ais_staticb',
  'ais_position', 'ais_positionb'
);
```

WAL equivalent GB/day from a 60 s delta of `pg_stat_wal.wal_bytes`:

```text
GB/day ≈ (wal_bytes_after - wal_bytes_before) * 1440 / 1e9
```

Probe: `SELECT 1` elapsed time, same threshold as the health endpoint (&lt; 100 ms healthy).

---

## What is still allowed to be noisy

These are known leftovers. They can push a sample out of the green column without the instance being too small.

1. **PyTSS-Reporting visit SQL** still seq-scans `ais_vesselinzone` (~6 scans/min, ~1.9 M rows). Intentionally not changed.
2. **`ais_vesselproximitymember`** — ~97 MB heap, ~815 MB primary key from delete+reinsert every 30 s. Index bloat, not CPU. `REINDEX INDEX CONCURRENTLY ais_vesselproximitymember_pkey` would reclaim it.
3. **`ais_ptp_vesselactivities`** (not in PyTSS / PySTS) was seen as a blank `application_name` writing `ais_vesselmovementactivities` and climbing toward the 60 s idle-in-transaction limit. Same long-session pattern as trajectory had.
4. After a systemd restart, a killed writer can leave an RDS backend `idle in transaction` for a minute or two. `lock_timeout` on the new process will fail those cycles until the leftover is `pg_terminate_backend`'d. That is fail-fast, not a capacity problem.

---

## Opinion on instance size

The previous (smaller) instance would handle the **current** query mix. `pnav` is 2.8 GB; the waste was 28 million rows/min of seq-scans, 45 GB/day of WAL from no-op updates, and transactions held open for minutes.

Do not roll the instance back. 16 GB is cheap headroom for reporting, `VACUUM`, and `CREATE INDEX CONCURRENTLY`. If health goes `degraded` again, read `application_name` on the idle-in-transaction session first.
