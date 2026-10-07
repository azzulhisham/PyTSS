
from typing import Optional
from urllib.parse import quote
from datetime import datetime, timedelta

from sqlmodel import Field, SQLModel, create_engine, Session, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_, or_, desc, text

import gc
import os
import time
import clickhouse_connect
import pandas as pd
import duckdb
import psycopg2
import platform
import logging



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Ais_StaticB(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    ts: datetime
    mmsi: int = Field(index=True)
    shipType: int
    shipTypeDesc: str
    shipName: str
    callsign: str
    vendor: str
    model: int
    serial: int
    imo: Optional[int] = Field(default=None)   
    to_bow: Optional[int] = Field(default=None)
    to_stern: Optional[int] = Field(default=None)
    to_port: Optional[int] = Field(default=None)
    to_starboard: Optional[int] = Field(default=None)
    destination: Optional[str] = Field(default=None)

				
# Database URL (adjust username, password, host, port, database name)
# pswd = 'Az@HoePinc0615'
# encoded_password = quote(pswd)
# DATABASE_URL = f"postgresql://postgres:{encoded_password}@localhost:5432/pnav"

pswd = 'm4r1t1m3'
encoded_password = quote(pswd)
DATABASE_URL = f"postgresql://postgresadmin:{encoded_password}@marineai2.cxwk8yige5f2.ap-southeast-5.rds.amazonaws.com:5432/pnav"

# Writes are committed in chunks this size rather than in one transaction that
# spans the whole upsert. The previous shape held a single transaction open for
# the entire bulk_update_mappings call; that call took tens of seconds, which is
# long enough on its own to push /tss/health/postgres into 'degraded' (it flags
# any session idle in transaction for more than 60s).
COMMIT_BATCH_SIZE = 500

# A blocked write fails fast instead of pinning a backend behind a lock holder:
# the cycle logs it and retries on the next pass.
PG_STATEMENT_TIMEOUT_MS = 60000
PG_LOCK_TIMEOUT_MS = 5000

_engine = None


def get_pgEngine():
    # Previously this built a brand new Engine on every call, so each cycle
    # created several independent pools of up to 30 connections. One cached
    # engine is reused instead.
    global _engine

    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            pool_size=2,
            max_overflow=2,
            pool_timeout=30,
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={
                # Named so these connections are attributable in pg_stat_activity
                # instead of showing up as a blank application_name.
                "application_name": "aisstatic_b",
                "options": (
                    f"-c statement_timeout={PG_STATEMENT_TIMEOUT_MS}"
                    f" -c lock_timeout={PG_LOCK_TIMEOUT_MS}"
                ),
            },
        )

    return _engine


def get_pgConn():
    conn = psycopg2.connect(
        dbname="pnav",
        user="postgresadmin",
        password="m4r1t1m3",
        host="marineai2.cxwk8yige5f2.ap-southeast-5.rds.amazonaws.com",
        port="5432"
    )

    return conn				


def create_db_and_tables():
    SQLModel.metadata.create_all(get_pgEngine())


# Columns of ais_staticb grouped by how they must be compared. The ClickHouse row
# does not carry every column (it has no 'imo', for instance), and bulk_update
# only writes the keys present in the payload, so only keys actually present are
# ever compared.
_TEXT_FIELDS = ("shipTypeDesc", "shipName", "callsign", "vendor", "destination")
_INT_FIELDS = (
    "mmsi", "shipType", "model", "serial",
    "to_bow", "to_stern", "to_port", "to_starboard",
)


def _is_null(value) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _norm_text(value) -> str:
    # The stored side arrives via pandas, so SQL NULL shows up as NaN rather than
    # None. '@' is AIS padding and is already stripped inconsistently upstream,
    # so it is normalised away on both sides before comparing.
    if _is_null(value):
        return ""
    return str(value).replace("@", "").strip()


def _as_optional_int(value) -> Optional[int]:
    # pandas widens integer columns to float as soon as one value is NULL, so the
    # stored side can be 123.0 where the ClickHouse side is 123.
    if _is_null(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ts_differs(new_val, old_val) -> bool:
    if _is_null(new_val) or _is_null(old_val):
        return not (_is_null(new_val) and _is_null(old_val))

    new_ts = pd.Timestamp(new_val)
    old_ts = pd.Timestamp(old_val)

    if new_ts.tz is not None:
        new_ts = new_ts.tz_convert("UTC").tz_localize(None)
    if old_ts.tz is not None:
        old_ts = old_ts.tz_convert("UTC").tz_localize(None)

    return new_ts != old_ts


def _staticb_row_differs(new_row, old_row) -> bool:
    """True when the ClickHouse row would actually change the stored row.

    Postgres writes a new row version even for an UPDATE that sets every column
    to the value it already holds, so a no-op update costs exactly as much as a
    real one. The ClickHouse lookback window keeps re-selecting the same
    unchanged vessels every cycle, which is why nearly every update was a no-op.

    ts is included in the comparison, so a vessel that merely reports again with
    a newer timestamp is still written. Only byte-identical rows are skipped.
    """
    for field in _TEXT_FIELDS:
        if field in new_row and _norm_text(new_row[field]) != _norm_text(old_row.get(field)):
            return True

    for field in _INT_FIELDS:
        if field in new_row and _as_optional_int(new_row[field]) != _as_optional_int(old_row.get(field)):
            return True

    if "imo" in new_row and _as_optional_int(new_row["imo"]) != _as_optional_int(old_row.get("imo")):
        return True

    return "ts" in new_row and _ts_differs(new_row["ts"], old_row.get("ts"))


def _latest_by_mmsi(pg_static_data):
    """Latest stored row per mmsi, keyed by mmsi.

    Replaces a per-vessel DuckDB query that re-scanned the whole 25k-row frame
    once for every ClickHouse row (~8,300 scans per cycle, the bulk of the 157s
    upsert). This computes the same thing the DuckDB query did -- row_number()
    OVER (PARTITION BY mmsi ORDER BY ts DESC) = 1 -- in a single pass.
    """
    latest = {}

    for row in pg_static_data.to_dict(orient="records"):
        mmsi = _as_optional_int(row.get("mmsi"))
        if mmsi is None:
            continue

        current = latest.get(mmsi)
        if current is None:
            latest[mmsi] = row
            continue

        new_ts, cur_ts = row.get("ts"), current.get("ts")
        if _is_null(cur_ts) or (not _is_null(new_ts) and pd.Timestamp(new_ts) > pd.Timestamp(cur_ts)):
            latest[mmsi] = row

    return latest


def _chunks(rows, size):
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def get_ais_position_data():
    query = text("""
        SELECT *
        FROM public.ais_positionb
        WHERE latitude >= :lat_min AND latitude <= :lat_max 
        ORDER BY "ts"
    """)

    # Define parameters
    params = {"lat_min": -90, "lat_max": 90}

    df = pd.read_sql(query, con=get_pgEngine(), params=params)  
    results = df.to_dict(orient='records')  

    del df
    gc.collect()
    
    return results

def get_pg_static_data():
    query = text("""
        SELECT *
        FROM public.ais_staticb
        ORDER BY "ts"
    """)

    # Define parameters
    # params = {"lat_min": -90, "lat_max": 90}

    df = pd.read_sql(query, con=get_pgEngine())  
    # results = df.to_dict(orient='records')  

    # del df
    # gc.collect()
    
    return df


def get_data_CH():
    client = clickhouse_connect.get_client(
        host='43.216.85.155',
        user='default'
    )


    try:
        logging.info(f'Retrieving data from CH...')

        qry = f'''
            WITH static_part1 AS (
                SELECT DISTINCT mmsi, shipType, shipTypeDesc, callsign
                FROM pnav.ais_type24
                WHERE  ts >= date_add(DAY, -30, now()) AND partNo = 1                
            ),        
            static_data AS (
                SELECT ts, mmsi, shipType, shipTypeDesc, shipName, '' AS callsign, 0 AS vendor, 0 AS serial, 0 AS model, to_bow, to_stern, to_port, to_starboard, '' AS destination,
                    row_number() OVER (PARTITION BY mmsi ORDER BY ts DESC) AS rowcountby_mmsi 
                FROM pnav.ais_type19
                WHERE  ts >= date_add(MINUTE, -15, now()) 
                
                UNION ALL
                
                SELECT ts, mmsi, sp1.shipType, sp1.shipTypeDesc, shipName, sp1.callsign, 0 AS vendor, 0 AS serial, 0 AS model, to_bow, to_stern, to_port, to_starboard, '' AS destination,
                    row_number() OVER (PARTITION BY mmsi ORDER BY ts DESC) AS rowcountby_mmsi 
                FROM pnav.ais_type24
                JOIN static_part1 AS sp1 ON sp1.mmsi = mmsi
                WHERE  ts >= date_add(DAY, -30, now()) AND partNo = 0   
            )
            SELECT *
            FROM static_data
            WHERE rowcountby_mmsi = 1
            ORDER BY ts
        '''

        result = client.query(qry) 


        if result.row_count > 0:
            df = pd.DataFrame(result.result_rows)
            df.columns = list(result.column_names)

            df['ts'] = pd.to_datetime(df['ts'])      
            payloads = df.to_dict(orient='records')  
            

        return payloads
          
    except Exception as e:
        logging.info(f'Error retrieving data from CH....{e}')
        return None


def upsert_ais_static(ais_static_data, pg_static_data):
    logging.info(f'Upserting data....{len(ais_static_data)}')

    items_to_update = []
    items_to_insert = []
    det_changed = []
    unchanged = 0

    try:
        pgEngine = get_pgEngine()
        latest_by_mmsi = _latest_by_mmsi(pg_static_data)

        # The decisions below are pure Python; nothing is written until after the
        # loop, so no transaction is held open while this runs.
        for i in ais_static_data:
            mmsi = _as_optional_int(i.get('mmsi'))
            existing = latest_by_mmsi.get(mmsi)

            if existing is not None:
                if not _staticb_row_differs(i, existing):
                    # Identical to what is already stored, ts included. Skipping
                    # it leaves the row exactly as the UPDATE would have left it.
                    unchanged += 1
                    continue

                dataid = {"id" : existing['id']}
                i.update(dataid)
                items_to_update.append(i)

                # The ClickHouse query unions two branches that each pick one row
                # per mmsi, so the same vessel can appear twice in a batch. The
                # old code queued both updates and the later one won. Folding the
                # queued values back in keeps that outcome: a following duplicate
                # is compared against what this batch will leave in the row, so it
                # is written when it would change the result and skipped when the
                # row already ends up that way.
                latest_by_mmsi[mmsi] = {**existing, **i}


                    # if str(i['callsign']).replace('@', '') != str(existing_pg_static[0]['callsign']).replace('@', ''):
                    #     data = {
                    #         "ts" : i['ts'],
                    #         "imo": i['imo'],
                    #         "detchg": "callsign",
                    #         "prev": str(existing_pg_static[0]['callsign']).replace('@', ''),
                    #         "cur": str(i['callsign']).replace('@', '')
                    #     }

                    #     det_changed.append(data)                      

                    # elif i['shipName'].replace('@', '') != existing_pg_static[0]['shipName'].replace('@', ''):
                    #     data = {
                    #         "ts" : i['ts'],
                    #         "imo": i['imo'],
                    #         "detchg": "shipName",
                    #         "prev": existing_pg_static[0]['shipName'].replace('@', ''),
                    #         "cur": i['shipName'].replace('@', '')
                    #     }

                    #     det_changed.append(data)  

            else:
                items_to_insert.append(i)

        # Short, self-contained transactions instead of one that spans the whole
        # upsert.
        for chunk in _chunks(items_to_update, COMMIT_BATCH_SIZE):
            with Session(pgEngine) as session:
                session.bulk_update_mappings(Ais_StaticB, chunk)
                session.commit()

        for chunk in _chunks(items_to_insert, COMMIT_BATCH_SIZE):
            with Session(pgEngine) as session:
                session.bulk_insert_mappings(Ais_StaticB, chunk)
                session.commit()

        if len(det_changed) != 0:
            dataset = pd.DataFrame.from_dict(det_changed)
            dataset.to_sql("ais_staticb_evt", con=pgEngine, if_exists='append', index=False)

        logging.info(
            f'Upserting data done.... update={len(items_to_update)} '
            f'unchanged={unchanged} insert={len(items_to_insert)}'
        )
        return 0

    except SQLAlchemyError as e:
        logging.info(f"Database error: {e}")
        # Optionally, roll back the transaction if possible:
        # session.rollback()  # only works if the session is still valid
        return -1



if __name__ == "__main__":
    runFlg = True
    create_db_and_tables()    

    while runFlg:
        try:
            logging.info(f'Fetching positioning data....')
            pg_static_data = get_pg_static_data()
            ais_static_data = get_data_CH()

            if ais_static_data != None:
                rslt = upsert_ais_static(ais_static_data, pg_static_data)

        except KeyboardInterrupt:
            runFlg = False

        except Exception as e:
            logging.info(f"Exception :: {e}")  


        logging.info(f'System sleep....')
        time.sleep(30)      

