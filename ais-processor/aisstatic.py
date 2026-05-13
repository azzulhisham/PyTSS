
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


class Ais_Static(SQLModel, table=True):
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

				
# Database URL (adjust username, password, host, port, database name)
# pswd = 'Az@HoePinc0615'
# encoded_password = quote(pswd)
# DATABASE_URL = f"postgresql://postgres:{encoded_password}@localhost:5432/pnav"

pswd = 'm4r1t1m3'
encoded_password = quote(pswd)
DATABASE_URL = f"postgresql://postgresadmin:{encoded_password}@marineai2.cxwk8yige5f2.ap-southeast-5.rds.amazonaws.com:5432/pnav"


def get_pgEngine():
    engine = create_engine(
        DATABASE_URL, 
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,  # seconds    
        # echo=True
    )  # echo=True for logging SQL

    return engine


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


def get_pg_static_data():
    query = text("""
        SELECT *
        FROM public.ais_static
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
        logging.info(f'Retrieving data from CH....')

        qry = f'''
            WITH static_data AS (
                SELECT ts, mmsi, shipType, shipTypeDesc, shipName, imo, callsign, to_bow, to_stern, to_port, to_starboard, destination, draught,
                    row_number() OVER (PARTITION BY imo ORDER BY ts DESC) AS rowcountby_imo
                FROM pnav.ais_static
                WHERE  ts >= date_add(MINUTE, -30, now())
            )
            SELECT *
            FROM static_data
            WHERE rowcountby_imo = 1
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
    logging.info(f'Upserting data....')

    items_to_update = []
    items_to_insert = []
    det_changed = []

    try:
        pgEngine = get_pgEngine()

        with Session(pgEngine) as session:
            for i in ais_static_data:
                # ais_position = Ais_Position(**i)
                # existing_ais: Ais_Static = session.exec(select(Ais_Static).where(Ais_Static.mmsi == i['mmsi'])).first()

                existing_pg_static = duckdb.sql(f'''
                    WITH pgstatic AS (
                        SELECT *,
                            row_number() OVER (PARTITION BY mmsi ORDER BY "ts" DESC) AS rowcount
                        FROM pg_static_data
                        WHERE imo = {i['imo']}
                    )
                    SELECT * 
                    FROM pgstatic 
                    WHERE rowcount = 1                    
                ''').fetchdf() 

                if len(existing_pg_static) > 0:
                    existing_pg_static = existing_pg_static.to_dict(orient='records')  

                    dataid = {"id" : existing_pg_static[0]['id']}
                    i.update(dataid)
                    items_to_update.append(i)

                    if i['imo'] not in [0, 1234567, 9999999, 12345678, 111111111, 123456789, 999999999]:
                        if i['mmsi'] != existing_pg_static[0]['mmsi']:
                            data = {
                                "ts" : i['ts'],
                                "imo": i['imo'],
                                "detchg": "mmsi",
                                "prev": str(existing_pg_static[0]['mmsi']),
                                "cur": str(i['mmsi'])
                            }

                            det_changed.append(data)

                        elif str(i['callsign']).replace('@', '') != str(existing_pg_static[0]['callsign']).replace('@', ''):
                            data = {
                                "ts" : i['ts'],
                                "imo": i['imo'],
                                "detchg": "callsign",
                                "prev": str(existing_pg_static[0]['callsign']).replace('@', ''),
                                "cur": str(i['callsign']).replace('@', '')
                            }

                            det_changed.append(data)

                        elif i['draught'] != existing_pg_static[0]['draught']:
                            data = {
                                "ts" : i['ts'],
                                "imo": i['imo'],
                                "detchg": "draught",
                                "prev": str(existing_pg_static[0]['draught']),
                                "cur": str(i['draught'])
                            }

                            det_changed.append(data)                        

                        elif i['shipName'].replace('@', '') != existing_pg_static[0]['shipName'].replace('@', ''):
                            data = {
                                "ts" : i['ts'],
                                "imo": i['imo'],
                                "detchg": "shipName",
                                "prev": existing_pg_static[0]['shipName'].replace('@', ''),
                                "cur": i['shipName'].replace('@', '')
                            }

                            det_changed.append(data)  

                else:
                    items_to_insert.append(i)


            session.bulk_update_mappings(Ais_Static, items_to_update)
            session.bulk_insert_mappings(Ais_Static, items_to_insert)
            session.commit() 

            dataset = pd.DataFrame.from_dict(det_changed)
            dataset.to_sql("ais_static_evt", con=pgEngine, if_exists='append', index=False) 

            logging.info(f'Upserting data done....')
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
        time.sleep(90)      

