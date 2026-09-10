-- One-off DDL for the aisposition / aisposition_b rewrite.
--
-- Adds a UNIQUE index on mmsi for both position tables. Both already hold exactly
-- one row per mmsi, so the build will succeed without conflicts.
--
-- CONCURRENTLY never blocks reads or writes, so this is safe to run with the
-- pipeline live. It cannot run inside a transaction block - execute each
-- statement on its own, not wrapped in BEGIN/COMMIT.
--
-- It waits for transactions that started before it to finish, twice. If a long
-- query or an "idle in transaction" session is open it will sit and wait; that
-- is waiting, not blocking, and nothing else is held up meanwhile.
--
-- RUN LOG 2026-09-09: ux_ais_position_mmsi built fine this way. The positionb
-- build stalled 12 min in "waiting for old snapshots" behind a session that
-- holds a long-lived transaction, and was finished with the plain build in
-- section 2b instead. Note the wait is for ANY older transaction in the
-- database, not just ones touching these tables.

-- ---------------------------------------------------------------------------
-- 1) Pre-flight: both must return 0 rows before you build the index
-- ---------------------------------------------------------------------------
SELECT mmsi, count(*)
FROM public.ais_position
GROUP BY mmsi HAVING count(*) > 1;

SELECT mmsi, count(*)
FROM public.ais_positionb
GROUP BY mmsi HAVING count(*) > 1;


-- ---------------------------------------------------------------------------
-- 2) Build the indexes (run one at a time, outside any transaction)
-- ---------------------------------------------------------------------------
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ux_ais_position_mmsi
    ON public.ais_position (mmsi);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ux_ais_positionb_mmsi
    ON public.ais_positionb (mmsi);


-- ---------------------------------------------------------------------------
-- 2b) Fallback if a CONCURRENTLY build stalls in "waiting for old snapshots".
--
--     Check what it is waiting for:
--         SELECT phase, blocks_done, blocks_total, current_locker_pid
--         FROM pg_stat_progress_create_index;
--
--     A plain build only needs a lock on the table itself, so it does not wait
--     on unrelated long transactions. At these row counts it takes ~1s, during
--     which writes to that one table pause. Cancel the stalled build and drop
--     the INVALID index it leaves behind first.
--
--     lock_timeout keeps it from queueing behind a lock holder and stacking
--     writes up behind it - on timeout nothing has changed and you can retry.
-- ---------------------------------------------------------------------------
-- SET lock_timeout = '5s';
-- CREATE UNIQUE INDEX ux_ais_positionb_mmsi ON public.ais_positionb (mmsi);
-- RESET lock_timeout;


-- ---------------------------------------------------------------------------
-- 3) Verify. indisvalid must be true for both.
--    If a build was interrupted it leaves an INVALID index: drop it and re-run.
-- ---------------------------------------------------------------------------
SELECT i.relname AS index_name, idx.indisvalid, idx.indisunique
FROM pg_index idx
JOIN pg_class i ON i.oid = idx.indexrelid
WHERE i.relname IN ('ux_ais_position_mmsi', 'ux_ais_positionb_mmsi');

-- DROP INDEX CONCURRENTLY ux_ais_position_mmsi;   -- only if indisvalid = false
-- DROP INDEX CONCURRENTLY ux_ais_positionb_mmsi;  -- only if indisvalid = false


-- ---------------------------------------------------------------------------
-- 4) Optional. The old non-unique indexes are fully redundant once the unique
--    ones exist - same column, same order. Dropping them removes write overhead
--    on every insert and update. Only do this after step 3 shows indisvalid.
-- ---------------------------------------------------------------------------
-- DROP INDEX CONCURRENTLY IF EXISTS public.ix_ais_position_mmsi;
-- DROP INDEX CONCURRENTLY IF EXISTS public.ix_ais_positionb_mmsi;
