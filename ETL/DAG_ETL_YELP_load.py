import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pandas as pd
import re
from google.cloud import bigquery

#from airflow.operators.http import HttpOperator
#from airflow.providers.google.cloud.operators.storage import ReadFromGoogleDriveFile
from airflow.providers.google.cloud.transfers.gdrive_to_local import GoogleDriveToLocalOperator




ruta_bucket = "gs://bucket-pghenry-dpt2"

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id="etl_yelp_load",
    default_args=default_args,
    description='ETL Yelp Load',
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)


def YELP_to_bigquery():

    # Instanciar BigQuery client
    client = bigquery.Client()

    #Crear dataset si no existe
    nombreDS = "pghenry-dpt2.yelp"
    dataset = bigquery.Dataset(nombreDS)
    dataset.location = "us-central1"
    client.create_dataset(dataset, exists_ok=True)

    tablas = ['business', 'tip', 'user', 'review', 'checkin']
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)

    for tabla in tablas:
        # table_id = "project.your_dataset.your_table_name"
        table_id = f'pghenry-dpt2.yelp.{tabla}'
        uri = f"{ruta_bucket}/filtrado/{tabla}.parquet"

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # API request.

        load_job.result()  # Espera que se complete el trabajo.

        destination_table = client.get_table(table_id)
        print(f"{destination_table.num_rows} filas cargadas")








# Tarea de carga de datos YELP procesados a Bigquery
load_data_bigquery_task = PythonOperator(
    task_id="load_data_to_bigquery",
    python_callable=YELP_to_bigquery,  
    dag=dag,
)



load_data_bigquery_task 