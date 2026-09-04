# pip install sqlmodel psycopg2

from typing import Optional
from urllib.parse import quote
from datetime import datetime, timedelta
from collections import defaultdict

from sqlmodel import Field, SQLModel, create_engine, Session
from sqlalchemy import text, Column, BigInteger, bindparam

import gc
import os
import time
import pandas as pd
import duckdb
import json
import logging

from polygons import *


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Keep DuckDB from taking too much CPU on the Linux host
duckdb.sql("SET threads TO 2")
duckdb.sql("SET memory_limit='1GB'")

# install / load spatial extension
duckdb.sql("INSTALL spatial")
duckdb.sql("LOAD spatial")


zones = [
    restrictedlimit_db,
    sector1limit_db,
    sector2limit_db,
    sector3limit_db,
    sector4limit_db,
    sector5limit_db,
    sector6limit_db,
    sector7limit_db,
    sector8limit_db,
    sector9limit_db,
    tssNouthbound_db,
    tssSouthbound_db,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    sector101limit_db,
    sector102limit_db,
    sector103limit_db,
    sector104limit_db,
    sector105limit_db,
    sector106limit_db
]


entire_tss_region = get_entire_tss_region_setting()
entire_sector789_region = get_entire_sector789_region_setting()

# Local watermark file (same folder as this script)
WATERMARK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vesselzone_watermark.txt")
WATERMARK_FMT = "%Y-%m-%d %H:%M:%S"
COMMIT_BATCH_SIZE = 300

# Rough bbox from the padded TSS region (SQL pre-filter only)
_tss_coords = entire_tss_region["coordinates"][0]
LON_MIN = min(c[0] for c in _tss_coords)
LON_MAX = max(c[0] for c in _tss_coords)
LAT_MIN = min(c[1] for c in _tss_coords)
LAT_MAX = max(c[1] for c in _tss_coords)


class Ais_Position(SQLModel, table=True):
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


class Ais_VesselInZone(SQLModel, table=True):
    id: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger, primary_key=True)
    )

    tsDetected: datetime
    mmsi: int = Field(index=True)
    navStatus: int
    navStatusDesc: str
    longitude: float
    latitude: float
    tsCurrent: Optional[datetime] = Field(default=None)
    tsOut: Optional[datetime] = Field(default=None)
    zone: Optional[int] = Field(default=None)
    imo: Optional[int] = Field(default=None)
    shipType: Optional[int] = Field(default=None)
    shipTypeDesc: Optional[str] = Field(default=None)
    shipName: Optional[str] = Field(default=None)
    callsign: Optional[str] = Field(default=None)
    destination: Optional[str] = Field(default=None)
    draught: Optional[float] = Field(default=None)
    to_bow: Optional[int] = Field(default=None)
    to_stern: Optional[int] = Field(default=None)
    to_port: Optional[int] = Field(default=None)
    to_starboard: Optional[int] = Field(default=None)
    curlongitude: Optional[float] = Field(default=None)
    curlatitude: Optional[float] = Field(default=None)
    sog: Optional[float] = Field(default=None)
    cog: Optional[float] = Field(default=None)
    rot: Optional[float] = Field(default=None)
    trueHeading: Optional[float] = Field(default=None)


# Database URL
pswd = 'm4r1t1m3'
encoded_password = quote(pswd)
DATABASE_URL = f"postgresql://postgresadmin:{encoded_password}@marineai2.cxwk8yige5f2.ap-southeast-5.rds.amazonaws.com:5432/pnav"

_engine = None


def get_pgEngine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
        )
    return _engine


def init_spatial_tables():
    """Parse zone / TSS polygons once and keep them in DuckDB."""
    zone_rows = []
    for idx, zone in enumerate(zones):
        if zone is None:
            continue
        zone_rows.append({"zone_id": idx, "geojson": json.dumps(zone)})

    zone_df = pd.DataFrame(zone_rows)
    duckdb.sql("""
        CREATE OR REPLACE TABLE zone_geoms AS
        SELECT zone_id, ST_GeomFromGeoJSON(geojson) AS geom
        FROM zone_df
    """)

    tss_df = pd.DataFrame([{"geojson": json.dumps(entire_tss_region)}])
    duckdb.sql("""
        CREATE OR REPLACE TABLE tss_region AS
        SELECT ST_GeomFromGeoJSON(geojson) AS geom
        FROM tss_df
    """)

    logging.info(f"Spatial tables ready :: {len(zone_rows)} zones")


def get_watermark(path=WATERMARK_PATH):
    try:
        with open(path, "r") as file:
            data = file.read().strip()
            if not data:
                return None
            return datetime.strptime(data, WATERMARK_FMT)
    except Exception:
        return None


def set_watermark(dt, path=WATERMARK_PATH):
    if dt is None:
        return
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    if getattr(dt, "tzinfo", None) is not None:
        dt = dt.replace(tzinfo=None)

    with open(path, "w") as file:
        file.write(f"{dt.strftime(WATERMARK_FMT)}\n")


def to_naive_datetime(value):
    if value is None or (isinstance(value, float) and pd.isnull(value)):
        return None
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if getattr(value, "tzinfo", None) is not None:
        value = value.replace(tzinfo=None)
    return value


def create_db_and_tables():
    SQLModel.metadata.create_all(get_pgEngine())


def get_ais_position_data():
    """
    Latest 1 row per MMSI.
    - No watermark file  -> last 1 DAY (safe bootstrap)
    - Watermark exists   -> only rows newer than last successful run
    """
    last_run_ts = get_watermark()

    if last_run_ts is None:
        logging.info("Watermark not found :: using last 1 DAY latest-per-MMSI")
        ts_clause = "p.ts >= NOW() - INTERVAL '1 DAY'"
        params = {
            "lon_min": LON_MIN,
            "lon_max": LON_MAX,
            "lat_min": LAT_MIN,
            "lat_max": LAT_MAX,
        }
    else:
        logging.info(f"Watermark found :: ts > {last_run_ts.strftime(WATERMARK_FMT)}")
        ts_clause = "p.ts > :ts_min"
        params = {
            "ts_min": last_run_ts,
            "lon_min": LON_MIN,
            "lon_max": LON_MAX,
            "lat_min": LAT_MIN,
            "lat_max": LAT_MAX,
        }

    query = text(f"""
        SELECT DISTINCT ON (p.mmsi)
            p.*,
            s.imo,
            s."shipType",
            s."shipTypeDesc",
            s."shipName",
            s.callsign,
            s.destination,
            s.draught,
            s.to_bow,
            s.to_stern,
            s.to_port,
            s.to_starboard
        FROM public.ais_position p
        LEFT JOIN public.ais_static s ON s.mmsi = p.mmsi
        WHERE {ts_clause}
          AND p.longitude BETWEEN :lon_min AND :lon_max
          AND p.latitude  BETWEEN :lat_min AND :lat_max
        ORDER BY p.mmsi, p.ts DESC
    """)

    df = pd.read_sql(query, con=get_pgEngine(), params=params)
    logging.info(f"Extracted latest positions :: {len(df)} rows")
    return df


def get_vessel_data():
    ais_data = get_ais_position_data()
    if ais_data is None or ais_data.empty:
        return []

    records = ais_data.to_dict(orient="records")
    del ais_data
    gc.collect()
    return records


def find_zones_for_points(data):
    """
    One DuckDB spatial join for the whole batch.
    Returns: {mmsi: {zone_id, ...}, ...}
    """
    if not data:
        return {}

    points_df = pd.DataFrame(data)
    duckdb.sql("""
        CREATE OR REPLACE TEMP TABLE batch_points AS
        SELECT mmsi, longitude, latitude
        FROM points_df
    """)

    result = duckdb.sql("""
        SELECT p.mmsi, z.zone_id
        FROM batch_points p
        INNER JOIN zone_geoms z
            ON ST_Within(ST_Point(p.longitude, p.latitude), z.geom)
    """).fetchdf()

    inside = defaultdict(set)
    if not result.empty:
        for mmsi, zone_id in zip(result["mmsi"].tolist(), result["zone_id"].tolist()):
            inside[int(mmsi)].add(int(zone_id))

    return inside


INT_COLUMNS = ("imo", "shipType", "to_bow", "to_stern", "to_port", "to_starboard")


def clean_record(record):
    # pandas turns NULL integer columns into NaN floats (e.g. imo=9312345.0),
    # which PostgreSQL rejects on UPDATE; normalize back to None/int
    cleaned = {}

    for k, v in record.items():
        if pd.isnull(v):
            cleaned[k] = None
        elif k in INT_COLUMNS:
            cleaned[k] = int(v)
        else:
            cleaned[k] = v

    return cleaned


def _prefer_existing(current_val, new_val):
    if current_val in (None, "") and new_val not in (None, "") and not (isinstance(new_val, float) and pd.isnull(new_val)):
        return new_val
    return current_val


def _flush_mappings(session, items_to_update, items_to_insert):
    if items_to_update:
        session.bulk_update_mappings(Ais_VesselInZone, items_to_update)
    if items_to_insert:
        session.bulk_insert_mappings(Ais_VesselInZone, items_to_insert)
    if items_to_update or items_to_insert:
        session.commit()
    items_to_update.clear()
    items_to_insert.clear()


def upsert_ais_position(data):
    logging.info(f"Upserting data....{len(data)}")

    if not data:
        logging.info("No new vessel positions to process")
        return None

    query = text("""
        SELECT *
        FROM public.ais_vesselinzone
        WHERE "tsOut" IS NULL
        ORDER BY "tsDetected" DESC
    """)

    try:
        df = pd.read_sql(query, con=get_pgEngine())
        current_vessels_zone = df.to_dict(orient="records")
        del df
        gc.collect()
    except Exception as e:
        logging.error("Error reading open zone records :: %s", e)
        return None

    open_by_key = {}
    open_by_mmsi = defaultdict(list)
    for row in current_vessels_zone:
        if not pd.isnull(row.get("tsOut")):
            continue
        key = (int(row["mmsi"]), int(row["zone"]))
        if key not in open_by_key:
            open_by_key[key] = row
            open_by_mmsi[key[0]].append(row)

    logging.info(f"Open zone records loaded :: {len(open_by_key)}")

    vessels_in_zones = find_zones_for_points(data)
    logging.info(
        f"Spatial join done :: {sum(len(z) for z in vessels_in_zones.values())} vessel-zone hits"
    )

    items_to_update = []
    items_to_insert = []
    insert_count = 0
    update_count = 0
    exit_count = 0
    max_ts = None
    now = datetime.now()

    with Session(get_pgEngine()) as session:
        for i in data:
            mmsi = int(i["mmsi"])
            vessel_ts = to_naive_datetime(i["ts"])
            if vessel_ts is not None and (max_ts is None or vessel_ts > max_ts):
                max_ts = vessel_ts

            zones_inside = vessels_in_zones.get(mmsi, set())

            # ENTER / STAY
            for idx in zones_inside:
                existing_vessel_zone = open_by_key.get((mmsi, idx))

                if existing_vessel_zone:
                    if pd.isnull(existing_vessel_zone.get("tsOut")):
                        ts_detected = to_naive_datetime(existing_vessel_zone["tsDetected"])
                        if (
                            ts_detected is not None
                            and now - ts_detected > timedelta(days=15)
                            and existing_vessel_zone["zone"] <= 11
                        ):
                            existing_vessel_zone["tsOut"] = now
                        else:
                            existing_vessel_zone["tsOut"] = None

                    payload = clean_record(existing_vessel_zone.copy())
                    payload["curlongitude"] = i["longitude"]
                    payload["curlatitude"] = i["latitude"]
                    payload["tsCurrent"] = vessel_ts
                    payload["destination"] = _prefer_existing(payload.get("destination"), i.get("destination"))
                    payload["sog"] = _prefer_existing(payload.get("sog"), i.get("sog"))
                    payload["cog"] = _prefer_existing(payload.get("cog"), i.get("cog"))
                    payload["rot"] = _prefer_existing(payload.get("rot"), i.get("rot"))
                    payload["trueHeading"] = _prefer_existing(payload.get("trueHeading"), i.get("trueHeading"))

                    items_to_update.append(payload)
                    update_count += 1
                else:
                    new_vessel_zone = {
                        "tsDetected": vessel_ts,
                        "mmsi": mmsi,
                        "navStatus": i["navStatus"],
                        "navStatusDesc": i["navStatusDesc"],
                        "longitude": i["longitude"],
                        "latitude": i["latitude"],
                        "tsCurrent": vessel_ts,
                        "tsOut": None,
                        "zone": idx,
                        "imo": i["imo"],
                        "shipType": i["shipType"],
                        "shipTypeDesc": i["shipTypeDesc"],
                        "shipName": i["shipName"],
                        "callsign": i["callsign"],
                        "destination": i["destination"],
                        "draught": i["draught"],
                        "to_bow": i["to_bow"],
                        "to_stern": i["to_stern"],
                        "to_port": i["to_port"],
                        "to_starboard": i["to_starboard"],
                        "curlongitude": i["longitude"],
                        "curlatitude": i["latitude"],
                        "sog": i["sog"],
                        "cog": i["cog"],
                        "rot": i["rot"],
                        "trueHeading": i["trueHeading"],
                    }
                    items_to_insert.append(clean_record(new_vessel_zone))
                    insert_count += 1

            # EXIT: open zones for this ship that are no longer matched
            for existing_vessel_zone in open_by_mmsi.get(mmsi, []):
                zone_id = int(existing_vessel_zone["zone"])
                if zone_id in zones_inside:
                    continue
                if not pd.isnull(existing_vessel_zone.get("tsOut")):
                    continue

                existing_vessel_zone["tsOut"] = vessel_ts
                payload = clean_record(existing_vessel_zone.copy())
                items_to_update.append(payload)
                exit_count += 1

            if len(items_to_update) + len(items_to_insert) >= COMMIT_BATCH_SIZE:
                _flush_mappings(session, items_to_update, items_to_insert)
                logging.info("Partial commit done....")

        _flush_mappings(session, items_to_update, items_to_insert)

    logging.info(
        f"Upserting data done :: insert={insert_count}, update={update_count}, exit={exit_count}"
    )

    del data
    gc.collect()
    return max_ts


def chk_invalid_data():
    logging.info("Clearing invalid data....")

    query = text("""
        SELECT p.mmsi, p.longitude, p.latitude
        FROM public.ais_position p
        WHERE p.mmsi IN (
            SELECT mmsi
            FROM public.ais_vesselinzone
            WHERE "tsOut" IS NULL
              AND "tsDetected" < NOW() - INTERVAL '15 days'
        )
    """)

    try:
        df = pd.read_sql(query, con=get_pgEngine())
        if df.empty:
            logging.info("No stale open-zone vessels to check")
            return 0

        duckdb.sql("""
            CREATE OR REPLACE TEMP TABLE stale_points AS
            SELECT mmsi, longitude, latitude
            FROM df
        """)

        outside_df = duckdb.sql("""
            SELECT s.mmsi
            FROM stale_points s
            CROSS JOIN tss_region t
            WHERE NOT ST_Within(ST_Point(s.longitude, s.latitude), t.geom)
        """).fetchdf()

        if outside_df.empty:
            logging.info("No stale vessels outside TSS")
            del df
            gc.collect()
            return 0

        mmsis = [int(x) for x in outside_df["mmsi"].tolist()]
        update_qry = text("""
            UPDATE public.ais_vesselinzone
            SET "tsOut" = now()
            WHERE mmsi IN :mmsis
              AND "tsOut" IS NULL
              AND "tsDetected" < NOW() - INTERVAL '15 days'
        """).bindparams(bindparam("mmsis", expanding=True))

        with get_pgEngine().begin() as conn:
            conn.execute(update_qry, {"mmsis": mmsis})

        logging.info(f"Closed stale open-zone records :: {len(mmsis)} mmsi")

        del df
        del outside_df
        gc.collect()

    except Exception as e:
        logging.error("chk_invalid_data FAILED :: %s", e, exc_info=True)

    return 0


if __name__ == "__main__":
    runFlg = True
    cycle_no = 0

    logging.info("[START] vesselzone service starting")
    create_db_and_tables()
    init_spatial_tables()
    logging.info("[START] ready :: watermark=%s", WATERMARK_PATH)

    while runFlg:
        cycle_no += 1
        stage = "init"
        t_cycle = time.perf_counter()
        row_count = 0

        try:
            stage = "fetch"
            logging.info("[CYCLE %s] stage=fetch :: loading latest positions", cycle_no)
            t0 = time.perf_counter()
            vessels_data = get_vessel_data()
            row_count = len(vessels_data)
            t_fetch = time.perf_counter() - t0
            logging.info(
                "[CYCLE %s] stage=fetch done :: rows=%s, elapsed=%.2fs",
                cycle_no, row_count, t_fetch,
            )

            stage = "upsert"
            logging.info("[CYCLE %s] stage=upsert :: zone check + write DB", cycle_no)
            t0 = time.perf_counter()
            max_ts = upsert_ais_position(vessels_data)
            t_upsert = time.perf_counter() - t0

            if max_ts is not None:
                set_watermark(max_ts)
                logging.info(
                    "[CYCLE %s] stage=upsert done :: watermark=%s, elapsed=%.2fs",
                    cycle_no, max_ts.strftime(WATERMARK_FMT), t_upsert,
                )
            else:
                logging.info(
                    "[CYCLE %s] stage=upsert done :: no new data, elapsed=%.2fs",
                    cycle_no, t_upsert,
                )

            del vessels_data
            gc.collect()

            stage = "cleanup"
            logging.info("[CYCLE %s] stage=cleanup :: stale open-zone check", cycle_no)
            t0 = time.perf_counter()
            chk_invalid_data()
            t_cleanup = time.perf_counter() - t0

            t_total = time.perf_counter() - t_cycle
            logging.info(
                "[CYCLE %s] OK :: rows=%s, fetch=%.2fs, upsert=%.2fs, cleanup=%.2fs, total=%.2fs",
                cycle_no, row_count, t_fetch, t_upsert, t_cleanup, t_total,
            )

        except KeyboardInterrupt:
            runFlg = False
            logging.info("[STOP] interrupted by user")

        except Exception as e:
            logging.error(
                "[CYCLE %s] FAILED at stage=%s :: %s",
                cycle_no, stage, e,
                exc_info=True,
            )

        if runFlg:
            logging.info("[CYCLE %s] sleep 2s", cycle_no)
            time.sleep(2)

    logging.info("[STOP] vesselzone service stopped")
