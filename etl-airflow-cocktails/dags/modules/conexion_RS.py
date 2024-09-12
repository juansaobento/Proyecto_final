import requests 
import json
import pandas as pd
import psycopg2 
import sqlalchemy as sa
import os
from dotenv import load_dotenv
from creacion_dataframe import creacion_dataframe

def conexion_RS():
    load_dotenv()
    REDSHIFT_USER=os.getenv('REDSHIFT_USER')
    REDSHIFT_PASSWORD=os.getenv('REDSHIFT_PASSWORD')
    REDSHIFT_HOST=os.getenv("REDSHIFT_HOST") 
    REDSHIFT_PORT=os.getenv('REDSHIFT_PORT',5439)
    REDSHIFT_DBNAME= os.getenv('REDSHIFT_DBNAME')
    TABLE_NAME_RS= 'juansaobento_cocktailsapi'
    SCHEMA_NAME_RS= 'juansaobento_coderhouse'
    conection_string=f"postgresql+psycopg2://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DBNAME}"
    db_engine= sa.create_engine(conection_string)
    df= creacion_dataframe()
    try:    
        df.to_sql(
            TABLE_NAME_RS,  
            db_engine, 
            schema= SCHEMA_NAME_RS,
            if_exists='append',
            index=False)
        
    except sa.exc.SQLAlchemyError as e:
        print(f"Error ocurred while droping the table: {e}")
    except Exception as e:
        print(e)