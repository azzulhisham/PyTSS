"""
Sync latest AIS class-B position data from ClickHouse -> Postgres public.ais_positionb.

The table holds one row per MMSI. Each cycle writes with batched statements
instead of a query per row:

  1) UPDATE ... FROM (VALUES ...)  for MMSIs already present
  2) INSERT ... WHERE NOT EXISTS   for genuinely new MMSIs only

A vessel stays in the live lookback window for many cycles, so the same fix is
re-picked repeatedly. The UPDATE compares every data column and writes only
when something actually differs, because Postgres otherwise creates a new row
version for an identical write. ts is part of that comparison, so a stationary
vessel reporting the same position with a fresh timestamp is still written; only
an exact duplicate of the stored message is skipped. The MMSIs already present
are read once per cycle, so the INSERT only sees genuinely new ones, and the
heartbeat row shares the write transaction: one commit per cycle.

Every setting has a default in _DEFAULTS below, so the script runs with no .env
file and no environment variables. Anything set in the environment wins.

INSERT ... ON CONFLICT is deliberately not used here: Postgres evaluates the id
default (nextval) before it detects the conflict, so it consumes one sequence
value for every row in the batch. ais_positionb.id is a 4-byte integer, and at
this cycle rate that would exhaust it within weeks.

Class-B (message 18) carries no navigation status and no rate of turn, so those
columns are filled with the same constants the previous version used.

Watermark file: crash-resume cursor (newest ts successfully seen). Catch-up
windows run only while that cursor is older than LIVE_LOOKBACK_MINUTES; once
near now, every cycle uses the rolling live lookback. LIVE_MODE_THRESHOLD is a
feed-health warning only — it does not switch modes.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional, Sequence
from urllib.parse import quote

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from psycopg2.extras import execute_values
from sqlmodel import Field, SQLModel, create_engine
from sqlalchemy.engine import Engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SCRIPT_DIR = Path(__file__).resolve().parent

PG_TABLE = "ais_positionb"
HEALTH_TABLE = "db_health_b"
CH_TABLE = "pnav.ais_type18"
MSG_TYPE = "position"

# Class-B message 18 has no navStatus / navStatusDesc / rot.
DEFAULT_NAV_STATUS = 0
DEFAULT_NAV_STATUS_DESC = ""
DEFAULT_ROT = 0.0

_DEFAULTS = {
    "PG_HOST": "marineai2.cxwk8yige5f2.ap-southeast-5.rds.amazonaws.com",
    "PG_PORT": "5432",
    "PG_USER": "postgresadmin",
    "PG_PASSWORD": "m4r1t1m3",
    "PG_DATABASE": "pnav",
    "CH_HOST": "56.69.44.39",
    "CH_PORT": "8123",
    "CH_USER": "default",
    "CH_PSWD": "Pinc@200901029426",
    "WATERMARK_FILE_B": "pnav_aisposition_b.txt",
    "WINDOW_SECONDS": "600",
    "LIVE_LOOKBACK_MINUTES": "10",
    "LIVE_MODE_THRESHOLD": "700",
    "POSITION_LOOP_SLEEP_SEC": "2",
    "ERROR_SLEEP_SEC": "12",
    "CHUNK_SIZE": "2000",
    "HEALTH_RETENTION_DAYS": "30",
    "HEALTH_PURGE_INTERVAL_HOURS": "24",
    "HEALTH_PURGE_CHUNK": "10000",
    "HEALTH_PURGE_MAX_SECONDS": "60",
    "PG_STATEMENT_TIMEOUT_MS": "30000",
    "PG_LOCK_TIMEOUT_MS": "5000",
    "CH_CONNECT_TIMEOUT_SEC": "15",
    "CH_SEND_RECEIVE_TIMEOUT_SEC": "60",
}

TS_FORMAT = "%Y-%m-%d %H:%M:%S"

# Columns read from ClickHouse. The remaining Postgres columns use the constants above.
CH_FIELDS = ("ts", "mmsi", "longitude", "latitude", "cog", "sog", "trueHeading")


class Ais_PositionB(SQLModel, table=True):
    __tablename__ = PG_TABLE

    id: Optional[int] = Field(default=None, primary_key=True)
    ts: datetime
    mmsi: int = Field(index=True)
    navStatus: int
    navStatusDesc: str
    longitude: float
    latitude: float
    rot: float
    cog: float
    sog: float
    trueHeading: float


def _load_dotenv(path: Path) -> None:
    """Minimal .env loader. Never overrides variables already in the environment."""
    if not path.is_file():
        return
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            if key and key not in os.environ:
                os.environ[key] = value
    except OSError as ex:
        logging.warning("Could not read %s: %s", path, ex)


def _cfg(name: str) -> str:
    return os.environ.get(name, _DEFAULTS[name])


def _cfg_int(name: str) -> int:
    try:
        return int(_cfg(name))
    except ValueError:
        logging.warning("Invalid int for %s, using default %s", name, _DEFAULTS[name])
        return int(_DEFAULTS[name])


_load_dotenv(SCRIPT_DIR / ".env")

DATABASE_URL = (
    f"postgresql+psycopg2://{_cfg('PG_USER')}:{quote(_cfg('PG_PASSWORD'), safe='')}"
    f"@{_cfg('PG_HOST')}:{_cfg('PG_PORT')}/{_cfg('PG_DATABASE')}"
)

CH_HOST = _cfg("CH_HOST")
CH_PORT = _cfg_int("CH_PORT")
CH_USER = _cfg("CH_USER")
CH_PSWD = _cfg("CH_PSWD")

WATERMARK_PATH = SCRIPT_DIR / _cfg("WATERMARK_FILE_B")
WINDOW_SECONDS = _cfg_int("WINDOW_SECONDS")
LIVE_LOOKBACK_MINUTES = _cfg_int("LIVE_LOOKBACK_MINUTES")
LIVE_MODE_THRESHOLD = _cfg_int("LIVE_MODE_THRESHOLD")
POSITION_LOOP_SLEEP_SEC = _cfg_int("POSITION_LOOP_SLEEP_SEC")
ERROR_SLEEP_SEC = _cfg_int("ERROR_SLEEP_SEC")
CHUNK_SIZE = _cfg_int("CHUNK_SIZE")
HEALTH_RETENTION_DAYS = _cfg_int("HEALTH_RETENTION_DAYS")
HEALTH_PURGE_INTERVAL_HOURS = _cfg_int("HEALTH_PURGE_INTERVAL_HOURS")
HEALTH_PURGE_CHUNK = _cfg_int("HEALTH_PURGE_CHUNK")
HEALTH_PURGE_MAX_SECONDS = _cfg_int("HEALTH_PURGE_MAX_SECONDS")
PG_STATEMENT_TIMEOUT_MS = _cfg_int("PG_STATEMENT_TIMEOUT_MS")
PG_LOCK_TIMEOUT_MS = _cfg_int("PG_LOCK_TIMEOUT_MS")
CH_CONNECT_TIMEOUT_SEC = _cfg_int("CH_CONNECT_TIMEOUT_SEC")
CH_SEND_RECEIVE_TIMEOUT_SEC = _cfg_int("CH_SEND_RECEIVE_TIMEOUT_SEC")


_SET_SQL = """\
    ts              = v.ts::timestamp,
    "navStatus"     = v.nav_status::integer,
    "navStatusDesc" = v.nav_status_desc::varchar,
    longitude       = v.longitude::double precision,
    latitude        = v.latitude::double precision,
    rot             = v.rot::double precision,
    cog             = v.cog::double precision,
    sog             = v.sog::double precision,
    "trueHeading"   = v.true_heading::double precision"""

_VALUES_SQL = """\
FROM (VALUES %s) AS v(
    ts, mmsi, nav_status, nav_status_desc,
    longitude, latitude, rot, cog, sog, true_heading
)"""

# A vessel stays inside the LIVE_LOOKBACK_MINUTES window for many cycles, so the
# same newest fix is re-picked over and over. Postgres never skips a no-op
# UPDATE by itself: it writes a new row version even when every value is
# identical, which bloats the table and keeps autovacuum busy for nothing. This
# predicate makes an unchanged vessel cost one comparison instead of a dead
# tuple, two index entries and the WAL for both.
#
# ts is compared too, so a stationary vessel reporting the same position with a
# fresh timestamp is still written: only a byte-identical duplicate is skipped.
# The class-B constants (navStatus / navStatusDesc / rot) are included as well,
# so a row left over from an older version is normalised on its first pass and
# matches from then on. The table has no triggers, rules, publications or
# dependent views, so a skipped identical write is not observable anywhere.
_CHANGED_SQL = """\
  AND (
        t.ts, t."navStatus", t."navStatusDesc",
        t.longitude, t.latitude, t.rot, t.cog, t.sog, t."trueHeading"
      ) IS DISTINCT FROM (
        v.ts::timestamp, v.nav_status::integer, v.nav_status_desc::varchar,
        v.longitude::double precision, v.latitude::double precision,
        v.rot::double precision, v.cog::double precision,
        v.sog::double precision, v.true_heading::double precision
      )"""

UPDATE_SQL = f"""
UPDATE public.{PG_TABLE} AS t SET
{_SET_SQL}
{_VALUES_SQL}
WHERE t.mmsi = v.mmsi::integer
  AND t.ts  <= v.ts::timestamp
{_CHANGED_SQL}
"""

INSERT_SQL = f"""
INSERT INTO public.{PG_TABLE}
    (ts, mmsi, "navStatus", "navStatusDesc", longitude, latitude, rot, cog, sog, "trueHeading")
SELECT
    v.ts::timestamp,
    v.mmsi::integer,
    v.nav_status::integer,
    v.nav_status_desc::varchar,
    v.longitude::double precision,
    v.latitude::double precision,
    v.rot::double precision,
    v.cog::double precision,
    v.sog::double precision,
    v.true_heading::double precision
FROM (VALUES %s) AS v(
    ts, mmsi, nav_status, nav_status_desc,
    longitude, latitude, rot, cog, sog, true_heading
)
WHERE NOT EXISTS (
    SELECT 1 FROM public.{PG_TABLE} t WHERE t.mmsi = v.mmsi::integer
)
"""

HEALTH_INSERT_SQL = f"""
INSERT INTO public.{HEALTH_TABLE} (ts, "msgType", "msgCnt") VALUES (%s, %s, %s)
"""

HEALTH_PURGE_SQL = f"""
DELETE FROM public.{HEALTH_TABLE}
WHERE ctid IN (
    SELECT ctid FROM public.{HEALTH_TABLE} WHERE ts < %s LIMIT %s
)
"""

# Which of this batch's MMSIs already have a row. Index-only scan on the unique
# mmsi index, and it replaces the per-cycle WHERE NOT EXISTS probe over the
# whole batch.
EXISTING_MMSI_SQL = f"""
SELECT mmsi
FROM public.{PG_TABLE}
WHERE mmsi = ANY(%s)
"""


_pg_engine: Optional[Engine] = None
_ch_client: Optional[Client] = None


def get_pg_engine() -> Engine:
    """One pooled engine. The loop is single-threaded, so it needs one connection.

    statement_timeout / lock_timeout make a blocked write fail fast instead of
    pinning a backend behind a lock holder: the cycle rolls back, sleeps
    ERROR_SLEEP_SEC and retries the same window, so nothing is lost. The
    watermark only advances after a successful commit.
    """
    global _pg_engine
    if _pg_engine is None:
        _pg_engine = create_engine(
            DATABASE_URL,
            pool_size=2,
            max_overflow=2,
            pool_timeout=30,
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={
                "application_name": "aisposition_b",
                "options": (
                    f"-c statement_timeout={PG_STATEMENT_TIMEOUT_MS}"
                    f" -c lock_timeout={PG_LOCK_TIMEOUT_MS}"
                ),
            },
        )
    return _pg_engine


def drop_ch_client() -> None:
    """Close and forget the client so the next cycle reconnects from scratch.

    Deliberately does not reconnect here. Rebuilding while ClickHouse is still
    down raises a connection error from inside the failure handler, which buries
    the original error and the traceback with it. Reconnecting lazily on the
    next cycle keeps the retry on the one path that already backs off.
    """
    global _ch_client
    if _ch_client is not None:
        try:
            _ch_client.close()
        except Exception:
            pass
    _ch_client = None


def get_ch_client() -> Client:
    """Reuse one ClickHouse client. The old code built one per loop and never closed it.

    CH_SEND_RECEIVE_TIMEOUT_SEC bounds how long a cycle can sit on a ClickHouse
    that accepted the connection but stopped answering. It has to be comfortably
    longer than a catch-up window query, and short enough that a hung server
    does not freeze a 2-second loop for minutes.
    """
    global _ch_client
    if _ch_client is None:
        _ch_client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PSWD,
            connect_timeout=CH_CONNECT_TIMEOUT_SEC,
            send_receive_timeout=CH_SEND_RECEIVE_TIMEOUT_SEC,
        )
    return _ch_client


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(get_pg_engine())


def read_watermark() -> Optional[datetime]:
    """Missing file, empty file and unparseable content all mean 'no watermark'."""
    try:
        raw = WATERMARK_PATH.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw:
        return None
    try:
        return datetime.strptime(raw, TS_FORMAT)
    except ValueError:
        logging.warning("Ignoring unparseable watermark %r in %s", raw, WATERMARK_PATH)
        return None


def write_watermark(value: Optional[datetime]) -> None:
    text = value.strftime(TS_FORMAT) if value is not None else ""
    try:
        WATERMARK_PATH.write_text(f"{text}\n", encoding="utf-8")
    except OSError as ex:
        logging.warning("Could not write watermark to %s: %s", WATERMARK_PATH, ex)


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def is_catching_up(watermark: Optional[datetime], now: Optional[datetime] = None) -> bool:
    """True when the resume point is older than the live lookback can cover.

    Watermark is always kept as a crash-resume cursor. Catch-up windows are only
    used when that cursor is behind the rolling live window; otherwise every
    cycle stays on the live lookback (avoids the 2s-loop sawtooth).
    """
    if watermark is None:
        return False
    now = now or _utcnow_naive()
    return watermark < now - timedelta(minutes=LIVE_LOOKBACK_MINUTES)


def build_query(watermark: Optional[datetime], catching_up: bool) -> str:
    if catching_up and watermark is not None:
        window_end = watermark + timedelta(seconds=WINDOW_SECONDS)
        where = (
            f"WHERE ts >= '{watermark.strftime(TS_FORMAT)}' "
            f"AND ts < '{window_end.strftime(TS_FORMAT)}'"
        )
    else:
        where = f"WHERE ts >= date_add(MINUTE, -{LIVE_LOOKBACK_MINUTES}, now())"

    # LIMIT 1 BY is ClickHouse's native "latest row per key" and is much cheaper
    # than row_number() OVER (PARTITION BY mmsi ORDER BY ts DESC).
    return f"""
        SELECT ts, mmsi, longitude, latitude, cog, sog, trueHeading
        FROM {CH_TABLE}
        {where}
        ORDER BY ts DESC
        LIMIT 1 BY mmsi
    """


def fetch_positions(watermark: Optional[datetime], catching_up: bool) -> list[tuple]:
    """Return rows shaped for the SQL VALUES clause, deduplicated on mmsi."""
    query = build_query(watermark, catching_up)
    logging.debug("ClickHouse query: %s", query)

    try:
        result = get_ch_client().query(query)
    except Exception:
        logging.exception("ClickHouse query failed, reconnecting on next cycle")
        drop_ch_client()
        raise

    columns = list(result.column_names)
    index = {name: columns.index(name) for name in CH_FIELDS}

    latest: dict[int, tuple] = {}
    skipped = 0
    for raw in result.result_rows:
        source = {name: raw[index[name]] for name in CH_FIELDS}
        if any(value is None for value in source.values()):
            skipped += 1
            continue

        mmsi = int(source["mmsi"])
        values = (
            source["ts"],
            mmsi,
            DEFAULT_NAV_STATUS,
            DEFAULT_NAV_STATUS_DESC,
            source["longitude"],
            source["latitude"],
            DEFAULT_ROT,
            source["cog"],
            source["sog"],
            source["trueHeading"],
        )
        previous = latest.get(mmsi)
        if previous is None or values[0] > previous[0]:
            latest[mmsi] = values

    if skipped:
        logging.warning("Skipped %s ClickHouse rows containing NULLs", skipped)

    return list(latest.values())


def load_existing_mmsis(mmsis: Sequence[int]) -> set[int]:
    """Which of these MMSIs already have a row in public.ais_positionb."""
    if not mmsis:
        return set()

    connection = get_pg_engine().raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(EXISTING_MMSI_SQL, (list(mmsis),))
            found = {int(row[0]) for row in cursor.fetchall()}
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return found


def split_rows(
    rows: Sequence[tuple], existing: set[int]
) -> tuple[list[tuple], list[tuple]]:
    """(to_update, to_insert). A row goes to INSERT only when its MMSI is new."""
    to_update: list[tuple] = []
    to_insert: list[tuple] = []
    for row in rows:
        if int(row[1]) in existing:
            to_update.append(row)
        else:
            to_insert.append(row)
    return to_update, to_insert


def _chunks(rows: Sequence[tuple], size: int) -> Iterator[Sequence[tuple]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def upsert_positions(
    update_rows: Sequence[tuple],
    insert_rows: Sequence[tuple] = (),
    health_count: Optional[int] = None,
) -> tuple[int, int]:
    """Returns (changed, inserted). Raises on failure so the caller can back off.

    changed counts rows Postgres actually wrote; rows already holding the same
    values are filtered by the IS DISTINCT FROM guard and cost nothing but a
    comparison. The heartbeat row rides in the same transaction, so a cycle
    commits once instead of three times.
    """
    if not update_rows and not insert_rows and health_count is None:
        return 0, 0

    changed = 0
    inserted = 0

    connection = get_pg_engine().raw_connection()
    try:
        with connection.cursor() as cursor:
            for chunk in _chunks(update_rows, CHUNK_SIZE):
                execute_values(cursor, UPDATE_SQL, chunk, page_size=len(chunk))
                changed += cursor.rowcount if cursor.rowcount > 0 else 0

            # WHERE NOT EXISTS stays as a safety net, but it now runs over the
            # few genuinely new MMSIs instead of the whole batch every cycle.
            for chunk in _chunks(insert_rows, CHUNK_SIZE):
                execute_values(cursor, INSERT_SQL, chunk, page_size=len(chunk))
                inserted += cursor.rowcount if cursor.rowcount > 0 else 0

            if health_count is not None:
                cursor.execute(
                    HEALTH_INSERT_SQL, (_utcnow_naive(), MSG_TYPE, health_count)
                )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return changed, inserted


def write_health(msg_count: int) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    connection = get_pg_engine().raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(HEALTH_INSERT_SQL, (now, MSG_TYPE, msg_count))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def purge_health() -> int:
    """Delete health rows older than the retention window, in bounded chunks."""
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
        days=HEALTH_RETENTION_DAYS
    )
    deadline = time.monotonic() + HEALTH_PURGE_MAX_SECONDS
    total = 0

    connection = get_pg_engine().raw_connection()
    try:
        while time.monotonic() < deadline:
            with connection.cursor() as cursor:
                cursor.execute(HEALTH_PURGE_SQL, (cutoff, HEALTH_PURGE_CHUNK))
                removed = cursor.rowcount
            connection.commit()
            if removed <= 0:
                break
            total += removed
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    if total:
        logging.info("Purged %s %s rows older than %s", total, HEALTH_TABLE, cutoff)
    return total


def run_cycle(watermark: Optional[datetime]) -> Optional[datetime]:
    """Process one window. Returns the watermark (resume cursor) to persist next.

    - Watermark always records the newest ts successfully seen, so a restart can
      resume without skipping a gap the live lookback cannot cover.
    - Catch-up mode runs only while that cursor is older than LIVE_LOOKBACK.
    - LIVE_MODE_THRESHOLD is a feed-health signal only; it no longer flips modes
      (that caused the high/low sawtooth on a ~2s loop).
    """
    now = _utcnow_naive()
    catching_up = is_catching_up(watermark, now)
    mode = "catch-up" if catching_up else "live"

    rows = fetch_positions(watermark, catching_up)
    count = len(rows)

    if count < LIVE_MODE_THRESHOLD:
        logging.warning(
            "Feed health: mode=%s rows=%s below threshold=%s — CH source may be down",
            mode,
            count,
            LIVE_MODE_THRESHOLD,
        )

    if count == 0:
        logging.info("No rows returned (mode=%s)", mode)
        write_health(0)
        # Empty catch-up window must still advance or the pipeline stalls forever.
        if catching_up and watermark is not None:
            return watermark + timedelta(seconds=WINDOW_SECONDS)
        # Live + empty: keep the existing resume cursor if we have one.
        return watermark

    existing = load_existing_mmsis([int(row[1]) for row in rows])
    to_update, to_insert = split_rows(rows, existing)
    # Heartbeat rides along, so the whole cycle is one commit.
    changed, inserted = upsert_positions(to_update, to_insert, health_count=count)
    newest = max(row[0] for row in rows)
    logging.info(
        "mode=%s rows=%s changed=%s unchanged=%s inserted=%s watermark=%s",
        mode,
        count,
        changed,
        max(0, len(to_update) - changed),
        inserted,
        newest.strftime(TS_FORMAT),
    )

    # Always persist newest as the resume point (live and catch-up alike).
    return newest


def main() -> None:
    create_db_and_tables()
    logging.info(
        "aisposition_b started (CH=%s:%s db=%s table=%s)",
        CH_HOST,
        CH_PORT,
        _cfg("PG_DATABASE"),
        PG_TABLE,
    )

    watermark = read_watermark()
    next_purge = 0.0
    run_flg = True

    while run_flg:
        try:
            if time.monotonic() >= next_purge:
                # Housekeeping must never stall position syncing. Schedule the
                # next attempt before trying, so a failure waits a full
                # interval instead of retrying every cycle and starving the
                # sync behind it.
                next_purge = time.monotonic() + HEALTH_PURGE_INTERVAL_HOURS * 3600
                try:
                    purge_health()
                except Exception:
                    logging.exception("Health purge failed, retrying next interval")

            watermark = run_cycle(watermark)
            write_watermark(watermark)

        except KeyboardInterrupt:
            run_flg = False
            logging.info("Interrupted")
            break

        except Exception:
            logging.exception("Cycle failed")
            time.sleep(ERROR_SLEEP_SEC)
            continue

        time.sleep(POSITION_LOOP_SLEEP_SEC)

    drop_ch_client()


if __name__ == "__main__":
    main()
