# pip install sqlmodel psycopg2

from typing import Optional
from urllib.parse import quote
from datetime import datetime, timedelta

from sqlmodel import Field, SQLModel, create_engine, Session, select
from sqlalchemy.exc import SQLAlchemyError

import gc
import os
import time
import clickhouse_connect
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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



# Database URL (adjust username, password, host, port, database name)
# pswd = 'Az@HoePinc0615'
# encoded_password = quote(pswd)
# DATABASE_URL = f"postgresql://postgres:{encoded_password}@localhost:5432/pnav"

pswd = 'm4r1t1m3'
encoded_password = quote(pswd)
DATABASE_URL = f"postgresql://postgresadmin:{encoded_password}@marineai2.cxwk8yige5f2.ap-southeast-5.rds.amazonaws.com:5432/pnav"


engine = create_engine(
    DATABASE_URL, 
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,  # seconds    
    # echo=True
)  # echo=True for logging SQL


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def upsert_ais_position(data):
    logging.info(f'Upserting data....')

    items_to_update = []
    items_to_insert = []

    try:
        with Session(engine) as session:
            for i in data:
                ais_position = Ais_Position(**i)
                existing_ais: Ais_Position = session.exec(select(Ais_Position).where(Ais_Position.mmsi == ais_position.mmsi)).first()

                if existing_ais:
                    dataid = {"id" : existing_ais.id}
                    i.update(dataid)
                    items_to_update.append(i)

                else:
                    items_to_insert.append(i)


            session.bulk_update_mappings(Ais_Position, items_to_update)
            session.bulk_insert_mappings(Ais_Position, items_to_insert)
            session.commit() 

            logging.info(f'Upserting data done....')
            return 0

    except SQLAlchemyError as e:
        logging.info(f"Database error: {e}")
        # Optionally, roll back the transaction if possible:
        # session.rollback()  # only works if the session is still valid
        return -1


def set_sys_seriesid(filename, data):
    with open(filename, 'w') as file:
        file.write(f"{data}\n")


def get_sys_seriesid(filename):
    try:
        with open(filename, 'r') as file:
            data = file.read()
            return data.strip()

    except:
        return None


if __name__ == "__main__":
    runFlg = True
    seriesid_filepath = 'pnav_aisposition.txt'

    create_db_and_tables()

    while runFlg:
        try:
            client = clickhouse_connect.get_client(
                host='43.216.85.155',
                user='default'
            )

            # where to begin
            set_where_clause = ''

            if os.path.exists(seriesid_filepath):
                start_seriesid = get_sys_seriesid(seriesid_filepath)

                if start_seriesid != None and start_seriesid != '':
                    dt = datetime.strptime(start_seriesid, "%Y-%m-%d %H:%M:%S")
                    dt = (dt + timedelta(seconds=600)).strftime("%Y-%m-%d %H:%M:%S")
                    set_where_clause = f"WHERE  ts >= '{start_seriesid}' AND ts < '{dt}'"
                else:
                    set_where_clause = 'WHERE  ts >= date_add(MINUTE, -10, now())'

            else:
                set_where_clause = 'WHERE  ts >= date_add(MINUTE, -10, now())'


            logging.info(f'Retrieving data....')

            qry = f'''
                WITH position_data AS (
                    SELECT ts, mmsi, navStatus, navStatusDesc, rot, sog, cog, trueHeading, longitude, latitude,
                        row_number() OVER (PARTITION BY mmsi ORDER BY ts DESC) AS rowcountby_mmsi 
                    FROM pnav.ais_position
                    {set_where_clause} 
                    ORDER BY ts
                )
                SELECT *
                FROM position_data
                WHERE rowcountby_mmsi = 1
                ORDER BY ts
                --LIMIT 1000
            '''

            logging.info(f"Execute :: {qry}")
            result = client.query(qry)

            if result.row_count > 0:
                logging.info(f'Transform data....{result.row_count}')
                df = pd.DataFrame(result.result_rows)
                df.columns = list(result.column_names)

                df['ts'] = pd.to_datetime(df['ts'])      
                payloads = df.to_dict(orient='records')  

                rslt = upsert_ais_position(payloads)

                if rslt == 0:
                    setData = payloads[len(payloads) - 1]['ts'].strftime("%Y-%m-%d %H:%M:%S")
                    logging.info(f'Set next start date....{setData}')
                    set_sys_seriesid(seriesid_filepath, setData)
            

                # save health check info to db
                health = [{
                    "ts": datetime.strptime(start_seriesid, "%Y-%m-%d %H:%M:%S"),
                    "msgType": 'position',
                    "msgCnt": result.row_count
                }]

                df_health = pd.DataFrame.from_dict(health)
                df_health.to_sql("db_health", con=engine, if_exists='append', index=False) 



                logging.info(f'Clearing memory....')
                del df
                del payloads
                del result
                gc.collect()

        except KeyboardInterrupt:
            runFlg = False

        except Exception as e:
            logging.info(f"Exception :: {e}")  
            time.sleep(12)

 
        logging.info(f'System sleep....')
        time.sleep(2)
        logging.info(f'................')

 

