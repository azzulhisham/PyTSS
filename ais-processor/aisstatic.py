"""
Sync latest AIS static data from ClickHouse -> Postgres public.ais_static.

Identity key: IMO first (normalized 7-digit + check digit); if IMO is null/invalid, use MMSI.
Also appends identity-change events to public.ais_static_evt.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver.client import Client
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Field, Session, SQLModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SCRIPT_DIR = Path(__file__).resolve().parent

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
    "CH_LOOKBACK_MINUTES": "30",
    "LOOP_SLEEP_SEC": "60",
    "IMO_CHECK_DIGIT": "1",
}

# Compared for ais_static_evt (AIS msg 5 / static fields).
CHANGE_FIELDS = (
    ("mmsi", "mmsi"),
    ("callsign", "callsign"),
    ("draught", "draught"),
    ("shipName", "shipName"),
    ("destination", "destination"),
    ("shipType", "shipType"),
    ("shipTypeDesc", "shipTypeDesc"),
)

PLACEHOLDER_IMOS = {1234567, 9999999, 12345678, 111111111, 123456789, 999999999}

STATIC_COLS = (
    "ts",
    "mmsi",
    "shipType",
    "shipTypeDesc",
    "shipName",
    "callsign",
    "imo",
    "to_bow",
    "to_stern",
    "to_port",
    "to_starboard",
    "destination",
    "draught",
)


class Ais_Static(SQLModel, table=True):
    __tablename__ = "ais_static"

    id: Optional[int] = Field(default=None, primary_key=True)
    ts: datetime
    mmsi: int = Field(index=True)
    shipType: int
    shipTypeDesc: str
    shipName: str
    callsign: str
    imo: Optional[int] = Field(default=None)
    to_bow: Optional[int] = Field(default=None)
    to_stern: Optional[int] = Field(default=None)
    to_port: Optional[int] = Field(default=None)
    to_starboard: Optional[int] = Field(default=None)
    destination: Optional[str] = Field(default=None)
    draught: Optional[float] = Field(default=None)


def _load_dotenv(path: Path) -> None:
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
    return int(_cfg(name))


def _cfg_bool(name: str) -> bool:
    return _cfg(name).strip().lower() in {"1", "true", "yes", "on"}


_load_dotenv(SCRIPT_DIR / ".env")

DATABASE_URL = (
    f"postgresql://{_cfg('PG_USER')}:{quote(_cfg('PG_PASSWORD'))}"
    f"@{_cfg('PG_HOST')}:{_cfg('PG_PORT')}/{_cfg('PG_DATABASE')}"
)
CH_HOST = _cfg("CH_HOST")
CH_PORT = _cfg_int("CH_PORT")
CH_USER = _cfg("CH_USER")
CH_PSWD = _cfg("CH_PSWD")
CH_LOOKBACK_MINUTES = _cfg_int("CH_LOOKBACK_MINUTES")
LOOP_SLEEP_SEC = _cfg_int("LOOP_SLEEP_SEC")
IMO_CHECK_DIGIT = _cfg_bool("IMO_CHECK_DIGIT")

_pg_engine: Optional[Engine] = None
_ch_client: Optional[Client] = None


def get_pg_engine() -> Engine:
    global _pg_engine
    if _pg_engine is None:
        _pg_engine = create_engine(
            DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
        )
    return _pg_engine


def get_ch_client() -> Client:
    global _ch_client
    if _ch_client is None:
        _ch_client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PSWD,
        )
    return _ch_client


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(get_pg_engine())


def imo_check_digit_valid(imo: int) -> bool:
    s = f"{int(imo):07d}"
    if len(s) != 7 or not s.isdigit():
        return False
    weights = (7, 6, 5, 4, 3, 2)
    total = sum(int(s[i]) * weights[i] for i in range(6))
    return total % 10 == int(s[6])


def normalize_imo(raw: Any, *, require_check_digit: Optional[bool] = None) -> Optional[int]:
    """Strip trailing zeros to 7 digits; optionally require IMO check digit."""
    if require_check_digit is None:
        require_check_digit = IMO_CHECK_DIGIT
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None

    s = str(value)
    while len(s) > 7 and s.endswith("0"):
        s = s[:-1]
    if len(s) != 7:
        return None

    imo = int(s)
    if require_check_digit and not imo_check_digit_valid(imo):
        return None
    return imo


def _is_null(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _as_optional_int(value: Any) -> Optional[int]:
    if _is_null(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _norm_text(value: Any) -> str:
    if _is_null(value):
        return ""
    return str(value).replace("@", "").strip()


def _values_differ(field: str, new_val: Any, old_val: Any) -> bool:
    if field in {"callsign", "shipName", "destination", "shipTypeDesc"}:
        return _norm_text(new_val) != _norm_text(old_val)
    if field == "draught":
        try:
            if _is_null(new_val) and _is_null(old_val):
                return False
            if _is_null(new_val) or _is_null(old_val):
                return True
            return float(new_val) != float(old_val)
        except (TypeError, ValueError):
            return str(new_val) != str(old_val)
    return new_val != old_val


def _clean_row(raw: dict[str, Any]) -> dict[str, Any]:
    row = {k: raw.get(k) for k in STATIC_COLS}
    row["imo"] = normalize_imo(raw.get("imo"))
    row["mmsi"] = int(raw["mmsi"])
    for key in ("shipType", "to_bow", "to_stern", "to_port", "to_starboard"):
        val = _as_optional_int(row.get(key))
        if key == "shipType":
            row[key] = 0 if val is None else val
        else:
            row[key] = val
    draught = row.get("draught")
    if _is_null(draught):
        row["draught"] = None
    else:
        row["draught"] = float(draught)
    for key in ("shipTypeDesc", "shipName", "callsign", "destination"):
        val = row.get(key)
        if _is_null(val):
            row[key] = None if key == "destination" else ""
        else:
            row[key] = str(val)
    return row


def dedupe_ch_static(df: pd.DataFrame) -> pd.DataFrame:
    """Latest row per IMO when valid; otherwise latest per MMSI."""
    if df.empty:
        return df

    out = df.copy()
    out["imo_norm"] = out["imo"].map(normalize_imo)
    out = out.sort_values("ts")

    with_imo = out[out["imo_norm"].notna()].drop_duplicates("imo_norm", keep="last")
    without_imo = out[out["imo_norm"].isna()].drop_duplicates("mmsi", keep="last")

    result = pd.concat([with_imo, without_imo], ignore_index=True)
    result["imo"] = result["imo_norm"]
    return result.drop(columns=["imo_norm"])


def get_data_ch() -> list[dict[str, Any]]:
    client = get_ch_client()
    logging.info(
        "Retrieving AIS static from CH %s (last %s min)...",
        CH_HOST,
        CH_LOOKBACK_MINUTES,
    )

    qry = f"""
        SELECT
            ts, mmsi, shipType, shipTypeDesc, shipName, imo, callsign,
            to_bow, to_stern, to_port, to_starboard, destination, draught
        FROM pnav.ais_static
        WHERE ts >= date_add(MINUTE, -{int(CH_LOOKBACK_MINUTES)}, now())
        ORDER BY ts
    """

    try:
        result = client.query(qry)
        if result.row_count == 0:
            logging.info("No CH static rows in lookback window")
            return []

        df = pd.DataFrame(result.result_rows, columns=list(result.column_names))
        df["ts"] = pd.to_datetime(df["ts"])
        df = dedupe_ch_static(df)
        logging.info("CH batch after dedupe: %s rows", len(df))
        return df.to_dict(orient="records")
    except Exception as ex:
        logging.exception("Error retrieving data from CH: %s", ex)
        return []


def get_pg_static_for_batch(
    batch: list[dict[str, Any]],
) -> tuple[dict[int, dict], dict[int, dict]]:
    """Load only Postgres rows needed for this batch. Returns (by_imo, by_mmsi_null_imo)."""
    imos: set[int] = set()
    mmsis: set[int] = set()
    for raw in batch:
        imo = normalize_imo(raw.get("imo"))
        if imo is not None:
            imos.add(imo)
        else:
            mmsis.add(int(raw["mmsi"]))

    by_imo: dict[int, dict] = {}
    by_mmsi: dict[int, dict] = {}
    if not imos and not mmsis:
        return by_imo, by_mmsi

    clauses: list[str] = []
    params: dict[str, Any] = {}
    if imos:
        clauses.append("imo = ANY(:imos)")
        params["imos"] = list(imos)
    if mmsis:
        clauses.append("(imo IS NULL AND mmsi = ANY(:mmsis))")
        params["mmsis"] = list(mmsis)

    query = text(f"""
        SELECT
            id, ts, mmsi, "shipType", "shipTypeDesc", "shipName", callsign, imo,
            to_bow, to_stern, to_port, to_starboard, destination, draught
        FROM public.ais_static
        WHERE {" OR ".join(clauses)}
        ORDER BY ts
    """)

    df = pd.read_sql(query, con=get_pg_engine(), params=params)
    for rec in df.to_dict(orient="records"):
        imo = _as_optional_int(rec.get("imo"))
        mmsi = _as_optional_int(rec.get("mmsi"))
        if imo is not None:
            rec["imo"] = imo
            by_imo[imo] = rec
        elif mmsi is not None:
            rec["imo"] = None
            by_mmsi[mmsi] = rec

    return by_imo, by_mmsi


def _find_existing(
    row: dict[str, Any],
    by_imo: dict[int, dict],
    by_mmsi: dict[int, dict],
) -> Optional[dict]:
    imo = _as_optional_int(row.get("imo"))
    if imo is not None:
        return by_imo.get(imo)
    mmsi = _as_optional_int(row.get("mmsi"))
    if mmsi is None:
        return None
    return by_mmsi.get(mmsi)


def _collect_changes(new_row: dict[str, Any], old_row: dict[str, Any]) -> list[dict[str, Any]]:
    imo = _as_optional_int(new_row.get("imo"))
    # Skip identity-change events when there is no usable IMO
    if imo is None or imo in PLACEHOLDER_IMOS:
        return []

    events: list[dict[str, Any]] = []
    for field, detchg in CHANGE_FIELDS:
        if not _values_differ(field, new_row.get(field), old_row.get(field)):
            continue
        prev = old_row.get(field)
        cur = new_row.get(field)
        if field in {"callsign", "shipName", "destination", "shipTypeDesc"}:
            prev = _norm_text(prev)
            cur = _norm_text(cur)
        else:
            prev = "" if prev is None else str(prev)
            cur = "" if cur is None else str(cur)
        events.append(
            {
                "ts": new_row["ts"],
                "imo": imo,
                "detchg": detchg,
                "prev": prev,
                "cur": cur,
            }
        )
    return events


def upsert_ais_static(ais_static_data: list[dict[str, Any]]) -> int:
    if not ais_static_data:
        logging.info("Nothing to upsert")
        return 0

    logging.info("Upserting %s AIS static rows...", len(ais_static_data))
    by_imo, by_mmsi = get_pg_static_for_batch(ais_static_data)

    items_to_update: list[dict[str, Any]] = []
    items_to_insert: list[dict[str, Any]] = []
    det_changed: list[dict[str, Any]] = []

    try:
        engine = get_pg_engine()
        with Session(engine) as session:
            for raw in ais_static_data:
                row = _clean_row(raw)
                existing = _find_existing(row, by_imo, by_mmsi)

                if existing is not None:
                    payload = dict(row)
                    payload["id"] = existing["id"]
                    items_to_update.append(payload)
                    det_changed.extend(_collect_changes(row, existing))
                    if row["imo"] is not None:
                        by_imo[int(row["imo"])] = {**existing, **payload}
                    else:
                        by_mmsi[int(row["mmsi"])] = {**existing, **payload}
                else:
                    items_to_insert.append(row)
                    if row["imo"] is not None:
                        by_imo[int(row["imo"])] = row
                    else:
                        by_mmsi[int(row["mmsi"])] = row

            if items_to_update:
                session.bulk_update_mappings(Ais_Static, items_to_update)
            if items_to_insert:
                session.bulk_insert_mappings(Ais_Static, items_to_insert)
            session.commit()

        if det_changed:
            pd.DataFrame(det_changed).to_sql(
                "ais_static_evt",
                con=engine,
                if_exists="append",
                index=False,
            )

        logging.info(
            "Upsert done: update=%s insert=%s events=%s",
            len(items_to_update),
            len(items_to_insert),
            len(det_changed),
        )
        return 0

    except SQLAlchemyError as ex:
        logging.exception("Database error: %s", ex)
        return -1


def main() -> None:
    create_db_and_tables()
    logging.info(
        "aisstatic started (CH=%s PG=%s/%s)",
        CH_HOST,
        _cfg("PG_HOST"),
        _cfg("PG_DATABASE"),
    )

    run_flg = True
    while run_flg:
        try:
            logging.info("Fetching AIS static data...")
            ais_static_data = get_data_ch()
            upsert_ais_static(ais_static_data)

        except KeyboardInterrupt:
            run_flg = False
            logging.info("Interrupted")
            break

        except Exception as ex:
            logging.exception("Exception: %s", ex)

        if run_flg:
            logging.info("Sleep %ss...", LOOP_SLEEP_SEC)
            time.sleep(LOOP_SLEEP_SEC)


if __name__ == "__main__":
    main()
