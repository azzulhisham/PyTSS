"""
Sync latest AIS position data from ClickHouse -> Postgres public.ais_position.

The table holds one row per MMSI. Each cycle writes with two batched statements
per chunk instead of a query per row:

  1) UPDATE ... FROM (VALUES ...)  for MMSIs already present
  2) INSERT ... WHERE NOT EXISTS   for genuinely new MMSIs only

INSERT ... ON CONFLICT is deliberately not used here: Postgres evaluates the id
default (nextval) before it detects the conflict, so it consumes one sequence
value for every row in the batch. ais_position.id is a 4-byte integer, and at
this cycle rate that would exhaust it within weeks.

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

PG_TABLE = "ais_position"
HEALTH_TABLE = "db_health"
CH_TABLE = "pnav.ais_position"
MSG_TYPE = "position"

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
    "WATERMARK_FILE": "pnav_aisposition.txt",
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
}

TS_FORMAT = "%Y-%m-%d %H:%M:%S"

# Order must match the VALUES aliases in UPDATE_SQL / INSERT_SQL below.
ROW_FIELDS = (
    "ts",
    "mmsi",
    "navStatus",
    "navStatusDesc",
    "longitude",
    "latitude",
    "rot",
    "cog",
    "sog",
    "trueHeading",
)


class Ais_Position(SQLModel, table=True):
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

WATERMARK_PATH = SCRIPT_DIR / _cfg("WATERMARK_FILE")
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


UPDATE_SQL = f"""
UPDATE public.{PG_TABLE} AS t SET
    ts              = v.ts::timestamp,
    "navStatus"     = v.nav_status::integer,
    "navStatusDesc" = v.nav_status_desc::varchar,
    longitude       = v.longitude::double precision,
    latitude        = v.latitude::double precision,
    rot             = v.rot::double precision,
    cog             = v.cog::double precision,
    sog             = v.sog::double precision,
    "trueHeading"   = v.true_heading::double precision
FROM (VALUES %s) AS v(
    ts, mmsi, nav_status, nav_status_desc,
    longitude, latitude, rot, cog, sog, true_heading
)
WHERE t.mmsi = v.mmsi::integer
  AND t.ts  <= v.ts::timestamp
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


_pg_engine: Optional[Engine] = None
_ch_client: Optional[Client] = None


def get_pg_engine() -> Engine:
    global _pg_engine
    if _pg_engine is None:
        _pg_engine = create_engine(
            DATABASE_URL,
            pool_size=5,
            max_overflow=5,
            pool_timeout=30,
            pool_pre_ping=True,
            pool_recycle=1800,
        )
    return _pg_engine


def get_ch_client(force_new: bool = False) -> Client:
    """Reuse one ClickHouse client. The old code built one per loop and never closed it."""
    global _ch_client
    if force_new and _ch_client is not None:
        try:
            _ch_client.close()
        except Exception:
            pass
        _ch_client = None
    if _ch_client is None:
        _ch_client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PSWD,
            connect_timeout=15,
            send_receive_timeout=300,
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
        SELECT ts, mmsi, navStatus, navStatusDesc, longitude, latitude, rot, cog, sog, trueHeading
        FROM {CH_TABLE}
        {where}
        ORDER BY ts DESC
        LIMIT 1 BY mmsi
    """


def fetch_positions(watermark: Optional[datetime], catching_up: bool) -> list[tuple]:
    """Return rows as tuples ordered like ROW_FIELDS, deduplicated on mmsi."""
    query = build_query(watermark, catching_up)
    logging.debug("ClickHouse query: %s", query)

    try:
        result = get_ch_client().query(query)
    except Exception:
        logging.exception("ClickHouse query failed, reconnecting on next cycle")
        get_ch_client(force_new=True)
        raise

    columns = list(result.column_names)
    index = {name: columns.index(name) for name in ROW_FIELDS}

    latest: dict[int, tuple] = {}
    skipped = 0
    for raw in result.result_rows:
        values = tuple(raw[index[name]] for name in ROW_FIELDS)
        if any(value is None for value in values):
            skipped += 1
            continue
        mmsi = int(values[1])
        previous = latest.get(mmsi)
        if previous is None or values[0] > previous[0]:
            latest[mmsi] = values

    if skipped:
        logging.warning("Skipped %s ClickHouse rows containing NULLs", skipped)

    return list(latest.values())


def _chunks(rows: Sequence[tuple], size: int) -> Iterator[Sequence[tuple]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def upsert_positions(rows: Sequence[tuple]) -> tuple[int, int]:
    """Returns (updated, inserted). Raises on failure so the caller can back off."""
    if not rows:
        return 0, 0

    updated = 0
    inserted = 0

    connection = get_pg_engine().raw_connection()
    try:
        with connection.cursor() as cursor:
            for chunk in _chunks(rows, CHUNK_SIZE):
                execute_values(cursor, UPDATE_SQL, chunk, page_size=len(chunk))
                updated += cursor.rowcount if cursor.rowcount > 0 else 0

                execute_values(cursor, INSERT_SQL, chunk, page_size=len(chunk))
                inserted += cursor.rowcount if cursor.rowcount > 0 else 0
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return updated, inserted


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

    updated, inserted = upsert_positions(rows)
    newest = max(row[0] for row in rows)
    logging.info(
        "mode=%s rows=%s updated=%s inserted=%s watermark=%s",
        mode,
        count,
        updated,
        inserted,
        newest.strftime(TS_FORMAT),
    )
    write_health(count)

    # Always persist newest as the resume point (live and catch-up alike).
    return newest


def main() -> None:
    create_db_and_tables()
    logging.info(
        "aisposition started (CH=%s:%s db=%s table=%s)",
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
                purge_health()
                next_purge = time.monotonic() + HEALTH_PURGE_INTERVAL_HOURS * 3600

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

    if _ch_client is not None:
        try:
            _ch_client.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
