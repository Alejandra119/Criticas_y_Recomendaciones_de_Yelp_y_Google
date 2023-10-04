import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pandas as pd
import re
import pyarrow.parquet as pq

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
def read_all_json_files(input_path):
    """Lee todos los archivos JSON en un directorio.

    Args:
        input_path (str): Ruta al directorio que contiene los archivos JSON.

    Returns:
        list[dict]: Una lista de diccionarios JSON.
    """

    json_files = []
    for filename in os.listdir(input_path):
        if filename.endswith('.json'):
            json_files.append(os.path.join(input_path, filename))

    json_data = []
    for json_file in json_files:
        with open(json_file, 'r') as f:
            json_data.append(json.load(f))

    return json_data

# Tarea para juntar todos los datos JSON en un solo DataFrame
def merge_json_data(json_data):
    """Junta todos los datos JSON en un solo DataFrame.

    Args:
        json_data (list[dict]): Una lista de diccionarios JSON.

    Returns:
        pandas.DataFrame: Un DataFrame que contiene todos los datos JSON.
    """

    df = pd.DataFrame(json_data[0])
    for i in range(1, len(json_data)):
        df = pd.concat([df, pd.DataFrame(json_data[i])])

    return df

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

def process_all_json_files(input_path):
    """Lee todos los archivos JSON en un directorio.

    Args:
        input_path (str): Ruta al directorio que contiene los archivos JSON.

    Returns:
        list[dict]: Una lista de diccionarios JSON.
    """

    json_files = []
    for filename in os.listdir(input_path):
        if filename.endswith('.json'):
            json_files.append(os.path.join(input_path, filename))

    json_data = []
    for json_file in json_files:
        with open(json_file, 'r') as f:
            json_data.append(json.load(f))

    return json_data

# Tarea para procesar todos los archivos JSON
t1 = PythonOperator(
    task_id='process_all_json_files',
    python_callable=process_all_json_files,
    op_args=['gs://bucket-pghenry-dpt2/google_sitios/'],
    dag=dag,
)

# Tarea para juntar todos los datos JSON en un solo DataFrame
t2 = PythonOperator(
    task_id='merge_json_data',
    python_callable=merge_json_data,
    op_args=[t1.output],
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
    op_args=[t3.output, 'gs://bucket-pghenry-dpt2/google/metadata_sitios.parquet'],
    dag=dag,
)

# Dependencias entre tareas
t1 >> t2 >> t3 >> t4

# Tarea para procesar todos los archivos JSON en review_estados
t5 = PythonOperator(
    task_id='process_all_json_files_2',
    python_callable=process_all_json_files,
    op_args=['gs://bucket-pghenry-dpt2/crudo/google_estados/review_Pennsylvania/'],
    dag=dag,
)

# Tarea para juntar todos los datos JSON en un solo DataFrame para review_estados
t6 = PythonOperator(
    task_id='merge_json_data_2',
    python_callable=merge_json_data,
    op_args=[t5.output],
    dag=dag,
)

# Tarea para procesar los datos JSON para la nueva carpeta
t7 = PythonOperator(
    task_id='process_json_data_2',
    python_callable=process_json_data,
    op_args=[t6.output],
    dag=dag,
)

# Tarea para escribir el DataFrame a un nuevo archivo Parquet
t8 = PythonOperator(
    task_id='write_dataframe_to_parquet_2',
    python_callable=write_dataframe_to_parquet,
    op_args=[t7.output, 'gs://bucket-pghenry-dpt2/review_estados.parquet'],
    dag=dag,
)

# Dependencias entre tareas para la nueva carpeta
t5 >> t6 >> t7 >> t8