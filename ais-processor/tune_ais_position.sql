-- Storage-side companion to the aisposition.py / aisposition_b.py write-path fix.
--
-- APPLIED 2026-10-07 on srv-01 / RDS marineai2 (db pnav). Both sections run.
--   fillfactor=70 set on both tables (instant, 0.009s / 0.013s, no contention)
--   VACUUM FULL ANALYZE: lock held 0.29s and 0.13s only, writers never blocked
--     ais_position   101 MB -> 9.8 MB    (heap 85 MB -> 7.8 MB, idx 16 MB -> 2.0 MB)
--     ais_positionb   39 MB -> 4.1 MB    (heap 33 MB -> 3.2 MB, idx 6.7 MB -> 944 kB)
--   measured after the code deploy, 60s live sample:
--     ais_position   1,006 -> 126 updates/sec  (-87.5%), HOT 16.4% -> 86%
--     ais_positionb    536 ->  12 updates/sec  (-97.4%), HOT 27.3% -> 97%
--     autovacuum  ~6 runs/90s -> 1 and 0
--   Services were NOT stopped. No errors in either journal.
--
-- WHY. public.ais_position holds one row per MMSI. Until the code fix, every
-- cycle rewrote every row it fetched, whether or not anything had changed:
--
--   measured 2026-10-07 on pnav
--     44,144 live rows holding 5.0 MB of real data
--     84.9 MB heap  (16.9x the data) + 16.1 MB of indexes
--     1,006 row updates/sec = ~86.9 M/day = each row rewritten ~1,970x/day
--     HOT updates only 16.4%, so ~84% also wrote new index entries
--     autovacuum ran 4x per minute on this table (453,348 runs lifetime)
--
-- ais_positionb is the same pattern on a smaller table: 20,671 rows, 536
-- updates/sec, HOT only 27.3%.
--
-- The code fix removes 86-98% of those writes (measured on live cycles). This
-- script deals with the two things code cannot fix: page packing, and the bloat
-- that already accumulated.
--
-- NOTHING HERE IS A PREREQUISITE FOR THE CODE. There is no new column, index
-- or type change, so the code behaves identically with or without this script.
-- The usual "migrate the schema before deploying" rule does not apply: this is
-- physical storage tuning only. The dependency runs the other way round - the
-- benefit of section 2 depends on the code already being deployed.
--
-- The writers do not need to be stopped for any of this. See each section.
--
-- The script's own startup call (SQLModel create_all) only checks that the
-- table exists; it emits no DDL, so fillfactor survives restarts and redeploys.
--
-- ORDER OF WORK
--   1. Section 1 (fillfactor) can run at any time, before or after the code
--      deploy. It is instant and helps the old code too.
--   2. Section 2 is OPTIONAL cleanup, and the only part where order matters:
--      run it AFTER the code is live, or the reclaimed space just re-bloats.
--      Read its warning first.
--
-- Run statements one at a time. VACUUM FULL cannot run inside a transaction
-- block, so do not wrap any of this in BEGIN/COMMIT.


-- ---------------------------------------------------------------------------
-- 0) Before: record the starting point so the change can be proven
-- ---------------------------------------------------------------------------
SELECT c.relname,
       (SELECT count(*) FROM public.ais_position)                AS live_rows,
       pg_size_pretty(pg_table_size(c.oid))                      AS heap,
       pg_size_pretty(pg_indexes_size(c.oid))                    AS indexes,
       c.reloptions
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relname = 'ais_position';

SELECT relname, n_live_tup, n_dead_tup, n_tup_upd, n_tup_hot_upd,
       round(100.0 * n_tup_hot_upd / GREATEST(n_tup_upd, 1), 1) AS hot_pct,
       autovacuum_count, last_autovacuum
FROM pg_stat_user_tables
WHERE schemaname = 'public' AND relname IN ('ais_position', 'ais_positionb');


-- ---------------------------------------------------------------------------
-- 1) fillfactor: let the remaining updates stay HOT
--
-- An update that does not change mmsi or id (none of ours do) can keep the new
-- row version in the same page and skip both indexes entirely - a HOT update.
-- That needs free space in the page, and the default fillfactor of 100 leaves
-- none, which is why only 16.4% of updates were HOT.
--
-- This is a catalog change: instant, no table rewrite, no exclusive lock held
-- for any meaningful time. It applies to pages written from now on, so HOT %
-- climbs gradually as rows are touched (or immediately after section 2).
--
-- 70 leaves room for roughly 20 row versions per page at this row width. Go to
-- 50 only if HOT % is still poor after a week.
--
-- Safe to run before OR after the code deploy: it changes no behaviour, only
-- how new pages are packed. Running it first is fine and mildly helps whatever
-- code is live at the time.
--
-- The writers do NOT need to be stopped. Their transactions last well under a
-- second, so the brief ACCESS EXCLUSIVE lock is granted almost immediately.
--
-- lock_timeout is the important part. The risk is not the ALTER being slow, it
-- is the ALTER *queueing* behind some other lock holder: once it waits, every
-- writer queues behind it too. On timeout nothing has changed, so just retry.
-- This database does carry occasional "idle in transaction" sessions, which is
-- exactly what that protects against.
-- ---------------------------------------------------------------------------
SET lock_timeout = '5s';

ALTER TABLE public.ais_position SET (fillfactor = 70);
ALTER TABLE public.ais_positionb SET (fillfactor = 70);

RESET lock_timeout;


-- ---------------------------------------------------------------------------
-- 2) OPTIONAL: reclaim the 16.9x bloat already on disk
--
-- Not urgent. Once the churn stops, the existing free space is simply reused
-- and the table stops growing; 85 MB is harmless on this instance. Only run
-- this if you want the space and the better cache hit ratio back.
--
-- WARNING. VACUUM FULL takes an ACCESS EXCLUSIVE lock: it rewrites the table
-- and rebuilds its indexes, and for those seconds nothing can read or write
-- ais_position - including the reporting API. At 101 MB total expect a few
-- seconds. It also needs room for a second copy while rewriting (101 MB for
-- ais_position, 39 MB for ais_positionb). pg_repack, the online alternative,
-- is not installed on this instance.
--
-- The writers still do not need to be stopped:
--   * with the new code, PG_LOCK_TIMEOUT_MS makes a blocked cycle give up
--     after 5s, roll back, sleep ERROR_SLEEP_SEC and retry;
--   * with the old code, the cycle simply waits until the rewrite finishes.
-- Either way nothing is lost, because the ClickHouse live window is 10 minutes
-- and absorbs a stall of a few seconds. Still, prefer a quiet moment: the
-- reporting API blocks on reads for the duration.
--
-- Run this AFTER section 1, so the rewrite packs pages at the new fillfactor,
-- and AFTER the code is deployed - rewriting while the old code still writes
-- ~1,006 rows/sec would re-bloat the table almost immediately.
--
-- Do not run it from a session with a short statement_timeout (pgAdmin
-- sometimes sets one), and not inside a transaction block.
--
-- lock_timeout here stops the VACUUM FULL from queueing behind a long reader
-- and stacking every writer up behind it. On timeout nothing has changed.
-- ---------------------------------------------------------------------------
-- SET lock_timeout = '5s';
-- VACUUM (FULL, ANALYZE, VERBOSE) public.ais_position;
-- VACUUM (FULL, ANALYZE, VERBOSE) public.ais_positionb;
-- RESET lock_timeout;


-- ---------------------------------------------------------------------------
-- 3) After: confirm it worked
--
-- Expect heap to fall towards single-digit MB if section 2 was run, hot_pct to
-- climb, and autovacuum_count to grow far more slowly than before (it was
-- adding ~8 runs/minute across the two position tables).
--
-- n_tup_upd is cumulative, so compare the delta over a fixed interval rather
-- than the absolute number.
-- ---------------------------------------------------------------------------
SELECT c.relname,
       pg_size_pretty(pg_table_size(c.oid))   AS heap,
       pg_size_pretty(pg_indexes_size(c.oid)) AS indexes,
       c.reloptions
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relname IN ('ais_position', 'ais_positionb');

-- Live update rate: run this, wait 60s, run it again, subtract.
SELECT now() AS at, relname, n_tup_upd, n_tup_hot_upd, n_dead_tup, autovacuum_count
FROM pg_stat_user_tables
WHERE schemaname = 'public' AND relname IN ('ais_position', 'ais_positionb');


-- ---------------------------------------------------------------------------
-- 4) Autovacuum: deliberately left at the defaults
--
-- Per-table autovacuum tuning was considered and is not needed. The cluster
-- runs autovacuum_vacuum_scale_factor = 0.1, so ~4.4k dead rows trigger a
-- vacuum here. The old problem was not the threshold, it was that ~86.9 M
-- updates/day kept crossing it continuously. With 86-97% of those writes gone,
-- the default threshold is reached rarely and autovacuum goes back to idle.
--
-- Re-check after a day with the query in section 3 before changing anything.
-- ---------------------------------------------------------------------------
