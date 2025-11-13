
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


def get_ais_position_data():
    query = text("""
        SELECT *
        FROM public.ais_position
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


def get_data_CH(vessels_data):
    client = clickhouse_connect.get_client(
        host='43.216.85.155',
        user='default'
    )

    data_ch = []

    try:
        logging.info(f'Retrieving data from CH....')
        cnt = 0
        tot = len(vessels_data)

        for i in vessels_data:
            qry = f'''
                WITH static_data AS (
                    SELECT ts, mmsi, shipType, shipTypeDesc, shipName, imo, callsign, to_bow, to_stern, to_port, to_starboard, destination,
                        row_number() OVER (PARTITION BY mmsi ORDER BY ts DESC) AS rowcountby_mmsi 
                    FROM pnav.ais_static
                    WHERE  ts >= date_add(MINUTE, -6, now()) AND mmsi = {i['mmsi']} 
                )
                SELECT *
                FROM static_data
                WHERE rowcountby_mmsi = 1
                ORDER BY ts
            '''

            result = client.query(qry) 
            cnt += 1   

            if result.row_count > 0:
                logging.info(f'Transform data....{((cnt/tot) * 100):.2f}%')
                df = pd.DataFrame(result.result_rows)
                df.columns = list(result.column_names)

                df['ts'] = pd.to_datetime(df['ts'])      
                payloads = df.to_dict(orient='records')  

                data_ch.append(payloads[0]) 


            time.sleep(0)
            

        return data_ch
          
    except Exception as e:
        logging.info(f'Error retrieving data from CH....{e}')
        return None


def upsert_ais_static(static_data):
    logging.info(f'Upserting data....')

    items_to_update = []
    items_to_insert = []

    try:
        with Session(get_pgEngine()) as session:
            for i in static_data:
                # ais_position = Ais_Position(**i)
                existing_ais: Ais_Static = session.exec(select(Ais_Static).where(Ais_Static.mmsi == i['mmsi'])).first()

                if existing_ais:
                    dataid = {"id" : existing_ais.id}
                    i.update(dataid)
                    items_to_update.append(i)

                else:
                    items_to_insert.append(i)


            session.bulk_update_mappings(Ais_Static, items_to_update)
            session.bulk_insert_mappings(Ais_Static, items_to_insert)
            session.commit() 

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
            vessels_data = get_ais_position_data()
            static_data = get_data_CH(vessels_data)

            if static_data != None:
                rslt = upsert_ais_static(static_data)

        except KeyboardInterrupt:
            runFlg = False

        except Exception as e:
            logging.info(f"Exception :: {e}")  


        logging.info(f'System sleep....')
        time.sleep(100)      

