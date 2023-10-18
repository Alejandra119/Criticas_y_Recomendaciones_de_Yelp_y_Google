import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pandas as pd
from datetime import datetime
import pyarrow.parquet as pq
from google.cloud import storage
import json
from Functions.elt_google import *


# import attr

# @attr.s
class MyDataFrame(pd.DataFrame):
    pass

ruta_bucket = 'gs://bucket-steakhouses2'


# Argumentos por defecto del DAG
default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id="etl_google_transform",
    default_args=default_args,
    description='ETL Google Transform',
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(days=1),
)


# Tarea para procesar todos los archivos JSON
t1 = PythonOperator(
    task_id='process_all_json_files',
    python_callable=process_all_json_files,
    op_args=['gs://bucket-steakhouses2/crudo/google'],
    op_kwargs={'output_path': 'gs://bucket-steakhouses2/crudo/results.json'},  # Añadir un argumento para la ruta de salida
    dag=dag,
)

# Tarea para juntar todos los datos JSON en un solo DataFrame
t2 = PythonOperator(
    task_id='merge_json_data',
    python_callable=merge_json_data,
    op_args=['gs://bucket-steakhouses2/crudo/results.json', 'gs://bucket-steakhouses2/filtrado/metadata_sitios.parquet'],  # Usar rutas de archivos en lugar de objetos
    dag=dag,
)

# Tarea para procesar los datos JSON
t3 = PythonOperator(
    task_id='process_json_data',
    python_callable=process_json_data,
    op_args=[t2.output],
    dag=dag,
)

# Tarea para escribir el DataFrame a un archivo Parquet
t4 = PythonOperator(
    task_id='write_dataframe_to_parquet',
    python_callable=write_dataframe_to_parquet,
    op_args=[t3.output, 'gs://bucket-steakhouses2/filtrado/metadata_sitios.parquet'],
    dag=dag,
)



# Dependencias entre tareas
t1 >> t2 >> t3 >> t4

t5 = PythonOperator(
    task_id='process_all_json_files_2',
    python_callable=process_all_json_files,
    op_args=['gs://bucket-steakhouses2/crudo/google/reviews-estados'],
    dag=dag,
)

# Tarea para procesar todos los archivos JSON en review_estados
t6 = PythonOperator(
    task_id='process_all_json_files_2',
    python_callable=process_all_json_files,
    op_args=['gs://bucket-steakhouses2/crudo/google/reviews-estados'],
    dag=dag,
)

# Tarea para juntar todos los datos JSON en un solo DataFrame para review_estados
t7 = PythonOperator(
    task_id='merge_json_data_2',
    python_callable=merge_json_data,
    op_args=[t5.output],
    dag=dag,
)

# Tarea para procesar los datos JSON para la nueva carpeta
t8 = PythonOperator(
    task_id='process_json_data_2',
    python_callable=process_json_data,
    op_args=[t6.output],
    dag=dag,
)

# Tarea para escribir el DataFrame a un nuevo archivo Parquet
t9 = PythonOperator(
    task_id='write_dataframe_to_parquet_2',
    python_callable=write_dataframe_to_parquet,
    op_args=[t7.output, 'gs://bucket-steakhouses2/filtrado/review_estados.parquet'],
    dag=dag,
)



def load_data_bq():

    # Instanciar BigQuery client
    client = bigquery.Client()

    #Crear dataset si no existe
    nombreDS = "steakhouses2.google"
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
        bigquery.SchemaField("last_date_inserted", "DATE"),
        bigquery.SchemaField("aditional_description", "STRING"),
        bigquery.SchemaField("aditional_value", "STRING"),          
    ]
    table_ref = client.dataset("google").table("audit")
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table, exists_ok=True)


    tablas = ['metadata_sitios', 'reviews_estados']
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)

    for tabla in tablas:
        table_id = f'steakhouses2.google.{tabla}'
        uri = f"{ruta_bucket}/filtrado/google/{tabla}.parquet"

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # API request.

        load_job.result()  # Espera que se complete el trabajo.

        destination_table = client.get_table(table_id)


        # Crear registro tabla auditoria
        audit = client.get_table('steakhouses2.google.audit')
        row = bigquery.Row([datetime.now(), tabla, "Carga inicial", 0, destination_table.num_rows, None, None, None], [])
        client.insert_rows(audit, [row])





# Dependencias entre tareas para la nueva carpeta
t5 >> t6 >> t7 >> t8 >> t9 >> load_data_bq