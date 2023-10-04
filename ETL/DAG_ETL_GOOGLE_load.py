import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
from datetime import datetime
import pandas as pd
import re
from google.cloud import bigquery

#from airflow.operators.http import HttpOperator
#from airflow.providers.google.cloud.operators.storage import ReadFromGoogleDriveFile
#from airflow.providers.google.cloud.transfers.gdrive_to_local import GoogleDriveToLocalOperator




ruta_bucket = "gs://bucket-pghenry-dpt2"

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id="etl_google_load",
    default_args=default_args,
    description='ETL Google Load',
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)


def GOOGLE_to_bigquery():

    # Instanciar BigQuery client
    client = bigquery.Client()

    #Crear dataset si no existe
    nombreDS = "pghenry-dpt2.google"
    dataset = bigquery.Dataset(nombreDS)
    dataset.location = "us-central1"
    client.create_dataset(dataset, exists_ok=True)

    tablas = ['metadata_sitios']
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)

    for tabla in tablas:
        # table_id = "proyecto.dataset.tabla"
        table_id = f'pghenry-dpt2.google.{tabla}'
        uri = f"{ruta_bucket}/google/{tabla}.parquet"

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # API request.

        load_job.result()  # Espera que se complete el trabajo.

        destination_table = client.get_table(table_id)


        # Crear registro tabla auditoria
        audit = client.get_table('pghenry-dpt2.google.audit')
        row = bigquery.Row([datetime.now(), tabla, None, None, destination_table.num_rows], [])
        client.insert_rows(audit, [row])




# Tarea de carga de datos GOOGLE procesados a Bigquery
load_google_bigquery_task = PythonOperator(
    task_id="load_google_to_bigquery",
    python_callable=GOOGLE_to_bigquery,  
    dag=dag,
)



load_google_bigquery_task 