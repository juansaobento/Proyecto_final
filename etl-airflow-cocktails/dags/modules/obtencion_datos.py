import requests 
import json
import pandas as pd
import psycopg2 
import sqlalchemy as sa
import os
from dotenv import load_dotenv

def obtencion_datos():
    url = 'https://www.thecocktaildb.com/api/json/v1/1/random.php'
    response = requests.get(url).json()
    data=response['drinks']
    datalist= pd.DataFrame(data)
    return(datalist)
    