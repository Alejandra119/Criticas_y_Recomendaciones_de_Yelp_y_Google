import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
from datetime import datetime
import pandas as pd
import re
from google.cloud import bigquery



ruta_bucket = "gs://bucket-steakhouses2"

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id="etl_yelp_load",
    default_args=default_args,
    description='ETL Yelp Load',
    schedule_interval= None, #"0 0 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)


def YELP_to_bigquery():

    # Instanciar BigQuery client
    client = bigquery.Client()

    #Crear dataset si no existe
    nombreDS = "steakhouses2.yelp"
    dataset = bigquery.Dataset(nombreDS)
    dataset.location = "us-central1"
    client.create_dataset(dataset, exists_ok=True)

    # Crear tabla audit si no existe
    schema = [
        bigquery.SchemaField("date_time", "TIMESTAMP"),
        bigquery.SchemaField("table_name", "STRING"),
        bigquery.SchemaField("task_name", "STRING"),
        bigquery.SchemaField("row_count_start", "INT64"),
        bigquery.SchemaField("row_count_end", "INT64"),
        bigquery.SchemaField("last_date_inserted", "DATE",),
        bigquery.SchemaField("aditional_description", "STRING"),
        bigquery.SchemaField("aditional_value", "STRING"),        
    ]
    table_ref = client.dataset("yelp").table("audit")
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table, exists_ok=True)


    # Crear tablas en BigQuery a partir de archivos
    tablas = ['business', 'tip', 'user', 'review', 'checkin']
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)

    for tabla in tablas:
        table_id = f'steakhouses2.yelp.{tabla}'
        uri = f"{ruta_bucket}/filtrado/{tabla}.parquet"

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # API request.

        load_job.result()  # Espera que se complete el trabajo.
        destination_table = client.get_table(table_id)

        # Crear registro tabla auditoria
        audit = client.get_table('steakhouses2.yelp.audit')
        row = bigquery.Row([datetime.now(), tabla, "Carga inicial", 0, destination_table.num_rows, None, None, None], [])
        client.insert_rows(audit, [row])



# Tarea de carga de datos YELP procesados a Bigquery
load_data_bigquery_task = PythonOperator(
    task_id="load_data_to_bigquery",
    python_callable=YELP_to_bigquery,  
    dag=dag,
)



load_data_bigquery_task 