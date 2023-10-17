import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pandas as pd
import re
import pyarrow.parquet as pq
import os
from google.cloud import storage
import json

import attr

@attr.s
class MyDataFrame(pd.DataFrame):
    pass

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

# Tarea para leer todos los archivos JSON
def process_all_json_files(input_path):
    """Lee todos los archivos JSON en un bucket de GCS.

    Args:
        input_path (str): Ruta al bucket y directorio que contiene los archivos JSON.

    Returns:
        list[dict]: Una lista de diccionarios JSON.
    """
    # Dividir la ruta de GCS en bucket y prefix
    bucket_name, prefix = input_path.replace("gs://", "").split("/", 1)

    # Crear un cliente de GCS
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    # Listar los blobs en el bucket usando el prefijo
    blobs = bucket.list_blobs(prefix=prefix)

    json_data = []
    for blob in blobs:
        # Leer el blob como una cadena y cargar el JSON
        json_str = blob.download_as_text()
        try:
            json_data.append(json.loads(json_str))
        except json.JSONDecodeError:
            print(f"Error decoding JSON from blob {blob.name}. Blob is empty or not valid JSON.")
            # O puedes usar logging en lugar de print para registrar el error
            # logging.error(f"Error decoding JSON from blob {blob.name}. Blob is empty or not valid JSON.")

    return json_data
# Tarea para juntar todos los datos JSON en un solo DataFrame
def merge_json_data(input_path, output_path):
    """Junta todos los datos JSON en un solo DataFrame.

    Args:
        input_path (str): Ruta al archivo que contiene los datos JSON.
        output_path (str): Ruta al archivo Parquet de salida.

    Returns:
        str: Ruta al archivo Parquet de salida.
    """
    # Leer los datos JSON desde input_path
    with open(input_path, 'r') as f:
        json_data = json.load(f)

    # ... [Resto de la lógica de la función]

    # Guardar el DataFrame como un archivo Parquet en output_path
    df.to_parquet(output_path, index=False)

    return output_path


# Tarea para procesar los datos JSON
def process_json_data(df):
    """Procesa los datos JSON en un DataFrame.

    Args:
        df (pandas.DataFrame): Un DataFrame que contiene los datos JSON.

    Returns:
        pandas.DataFrame: Un DataFrame que contiene los datos JSON procesados.
    """

    # Aquí puedes agregar el código para procesar los datos JSON. Por ejemplo, puedes eliminar columnas, transformar datos, etc.

    return df

# Tarea para escribir el DataFrame a un archivo Parquet
def write_dataframe_to_parquet(df, output_path):
    """Escribe un DataFrame a un archivo Parquet.

    Args:
        df (pandas.DataFrame): Un DataFrame que contiene los datos.
        output_path (str): Ruta al archivo Parquet.
    """

    df.to_parquet(output_path, index=False)

def check_files_exist():
    """Comprueba si los archivos JSON están disponibles.

    Returns:
        bool: True si los archivos JSON están disponibles, False si no.
    """
    # Obtener la lista de archivos JSON
    blob_names = ["gs://bucket-steakhouses2/crudo/google/reviews-estados/" + blob.name for blob in bucket.list_blobs(prefix="")]

    # Verificar si los archivos JSON existen
    for blob_name in blob_names:
        if not bucket.get_blob(blob_name).exists():
            return False

    return True

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

# Dependencias entre tareas para la nueva carpeta
t5 >> t6 >> t7 >> t8 >> t9