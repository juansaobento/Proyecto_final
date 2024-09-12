import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from modules import obtencion_datos,creacion_dataframe,conexion_RS, envio_email
from datetime import datetime, timedelta

default_args={
    'owner': 'juansaobento',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries':3,
    'retry_delay': timedelta(minutes=3)
}   

with DAG (
    default_args=default_args,
    dag_id="etl-cocktails",
    schedule="@daily",
    start_date=dt.datetime(year=2024, month=8, day=25),
    end_date=None,
    catchup=True,
    tags=["etl", "cocktails"],
    doc_md="Este dag permite extraer y cargar cocktails"
    ) as dag:

    obtencion_datos= PythonOperator(
            dag= dag,
            task_id="obtencion-operator",
            python_callable= obtencion_datos
            )
    
    creacion_df= PythonOperator(
            dag= dag,
            task_id="creacion-data-frame",
            python_callable= creacion_dataframe
            )
    
    conexion_RS= PythonOperator(
            dag= dag,
            task_id="conexion-rs",
            python_callable= conexion_RS
            )
    
    envio_email= PythonOperator(
            dag= dag,
            task_id="envio-email",
            python_callable= envio_email
            )

    
    obtencion_datos>>creacion_df>>conexion_RS>>envio_email