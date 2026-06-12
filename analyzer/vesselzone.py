# pip install sqlmodel psycopg2

from typing import Optional
from urllib.parse import quote
from datetime import datetime, timedelta, UTC

from sqlmodel import Field, SQLModel, create_engine, Session, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_, or_, desc, text, Column, BigInteger

import gc
import os
import time
import pandas as pd
import duckdb
import psycopg2
import json
import platform
import logging

from polygons import *


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# install duckdb extensions
# wget http://extensions.duckdb.org/v1.2.0/linux_amd64_gcc4/spatial.duckdb_extension.gz
duckdb.sql("INSTALL spatial")

# loading spatial extension
duckdb.sql("LOAD spatial")


# if platform.processor().lower() == 'arm':
#     duckdb.sql("LOAD './analyzer/spatial.duckdb_extension_osx_arm64'")     # for MacOS
# else:
#     duckdb.sql("LOAD './analyzer/spatial.duckdb_extension'")     # for Linux



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
    # id: Optional[int] = Field(default=None, primary_key=True)
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
    # results = None
    # with Session(engine) as session:
    #     statement = (
    #         select(Ais_Position)
    #         .where(and_(Ais_Position.latitude >= -90, Ais_Position.latitude <= 90))
    #         .order_by(Ais_Position.ts)  
    #         # .limit(1000)  
    #     )

    #     results = session.exec(statement).all()

    query = text("""
        SELECT p.*, s.imo, s."shipType", s."shipTypeDesc", s."shipName", s.callsign, 
            s.destination, s.draught, s.to_bow, s.to_stern, s.to_port, s.to_starboard
        FROM public.ais_position p
		LEFT JOIN public.ais_static s ON s.mmsi = p.mmsi
        WHERE p.latitude >= :lat_min AND p.latitude <= :lat_max AND p.ts >= NOW() - INTERVAL '2 DAYS'
        ORDER BY p.ts
    """)

    # Define parameters
    params = {"lat_min": -90, "lat_max": 90}
    df = pd.read_sql(query, con=get_pgEngine(), params=params)  

    return df


def get_vessel_data():
    ais_data = get_ais_position_data()

    df = duckdb.sql(f'''
        SELECT *
        FROM ais_data
        WHERE ST_Within(ST_Point(longitude, latitude), ST_GeomFromGeoJSON({entire_tss_region})) 
            -- OR ST_Within(ST_Point(longitude, latitude), ST_GeomFromGeoJSON({entire_sector789_region}))
    ''').fetchdf()

    del ais_data
    gc.collect()

    return df.to_dict(orient='records') 


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


def upsert_ais_position(data):
    logging.info(f'Upserting data....{len(data)}')

    items_to_update = []
    items_to_insert = []
    current_vessels_zone = []

    query = text("""
        SELECT *
        FROM public.ais_vesselinzone
        WHERE "tsOut" IS NULL
        ORDER BY "tsDetected" DESC
    """)

    try:
        df = pd.read_sql(query, con=get_pgEngine())  
        current_vessels_zone = df.to_dict(orient='records')   

        del df
        gc.collect()
    except:
        logging.info(f'Error reading database....')
        return 0


    with Session(get_pgEngine()) as session:
        logging.info(f'Loading data....{len(current_vessels_zone)}')

        for cnt, i in enumerate(data):
            # ais_position = Ais_Position(**i)   

            for idx, zone in enumerate(zones):
                if zone == None: continue

                rslt = duckdb.sql(f'''
                    SELECT ST_Within(ST_Point({i['longitude']}, {i['latitude']}), ST_GeomFromGeoJSON({zone})) as within_area
                ''').fetchall()       

                in_zone = rslt[0][0] 
                existing_vessel_zone = next(filter(lambda x: x["mmsi"] == i['mmsi'] and x["zone"] == idx and pd.isnull(x['tsOut']), current_vessels_zone), None)      

                if in_zone:
                    if existing_vessel_zone:
                        logging.info(f"[UPDATE] :: vessel {i['mmsi']} in zone {existing_vessel_zone['zone']}")
                        
                        if pd.isnull(existing_vessel_zone['tsOut']): 
                            if datetime.now() - existing_vessel_zone['tsDetected'] > timedelta(days=6) and existing_vessel_zone['zone'] <= 11:
                                existing_vessel_zone['tsOut'] = datetime.now() 
                            else:
                                existing_vessel_zone['tsOut'] = None 
                        
                        payload = clean_record(existing_vessel_zone)      #.model_dump()

                        payload["longitude"] = i['longitude']
                        payload["latitude"] = i['latitude'] 
                        payload["tsCurrent"] = i['ts']                                     
                        items_to_update.append(payload)                         

                    else:
                        logging.info(f"[INSERT] :: vessel {i['mmsi']} entered zone {idx}")
                        new_vessel_zone = {
                            "tsDetected": i['ts'],
                            "mmsi": i['mmsi'],
                            "navStatus": i['navStatus'],
                            "navStatusDesc": i['navStatusDesc'],
                            "longitude": i['longitude'],
                            "latitude": i['latitude'], 
                            "tsCurrent": i['ts'],
                            "tsOut": None,
                            "zone": idx                       
                        }

                        items_to_insert.append(new_vessel_zone)
                else:                    
                    if existing_vessel_zone:
                        logging.info(f"[UPDATE] :: vessel {i['mmsi']} exit zone {existing_vessel_zone['zone']}")
                        existing_vessel_zone["tsOut"] = i['ts']

                        payload = clean_record(existing_vessel_zone)      #.model_dump()
                        items_to_update.append(payload)                


            if cnt % 300 == 0:
                logging.info(f'Partially commiting to database....')
                session.bulk_update_mappings(Ais_VesselInZone, items_to_update)
                session.bulk_insert_mappings(Ais_VesselInZone, items_to_insert)
                session.commit() 

                items_to_update.clear()
                items_to_update = []
                items_to_insert.clear()
                items_to_insert = []

                logging.info(f'Partially upserting data done....')


        logging.info(f'Commiting to database....')
        if len(items_to_update) != 0: session.bulk_update_mappings(Ais_VesselInZone, items_to_update)
        if len(items_to_insert) != 0: session.bulk_insert_mappings(Ais_VesselInZone, items_to_insert)
        if len(items_to_update) != 0 or len(items_to_insert) != 0: session.commit() 


    logging.info(f'Upserting data done....')
    
    del data
    gc.collect()

    return 0            


def chk_invalid_data():
    logging.info(f'Clearing invalid data....')

    current_vessels_inrec = []

    query = text("""
        SELECT *
        FROM public.ais_position
        WHERE mmsi in (
            SELECT mmsi
            FROM public.ais_vesselinzone
            WHERE "tsOut" IS NULL
                AND "tsDetected" < NOW() - INTERVAL '5 days'
        )
    """)

    try:
        df = pd.read_sql(query, con=get_pgEngine())  
        current_vessels_inrec = df.to_dict(orient='records')   

        for idx, itm in enumerate(current_vessels_inrec):
            rslt = duckdb.sql(f'''
                SELECT ST_Within(ST_Point({itm['longitude']}, {itm['latitude']}), ST_GeomFromGeoJSON({entire_tss_region})) as within_area
            ''').fetchall()       

            in_zone = rslt[0][0]  

            if not in_zone:
                with get_pgEngine().connect() as conn:
                    # Write your raw SQL UPDATE statement
                    update_qry = text(f"""
                        UPDATE public.ais_vesselinzone
                        SET "tsOut" = now()   
                        WHERE mmsi = :mmsi
                            AND "tsOut" IS NULL
                            AND "tsDetected" < NOW() - INTERVAL '5 days'
                    """)

                    # Execute the statement with parameters
                    conn.execute(update_qry, {
                        "mmsi": itm['mmsi']
                    })

                    # Commit the transaction
                    logging.info(f'Updating data for mmsi: {itm["mmsi"]}')
                    conn.commit()  


        del df
        gc.collect()  

    except:
        pass


    return 0


if __name__ == "__main__":
    runFlg = True
    create_db_and_tables()    

    while runFlg:
        try:
            logging.info(f'Fetching data....')
            vessels_data = get_vessel_data()
            rslt = upsert_ais_position(vessels_data)

            del vessels_data
            gc.collect()

            chk_invalid_data()

        except KeyboardInterrupt:
            runFlg = False

        except Exception as e:
            logging.info(f"Exception :: {e}")  


        logging.info(f'System sleep....')
        time.sleep(1)     
       









