import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from Functions.elt_google import *



ruta_bucket = "gs://bucket-steakhouses2"

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id="google_as",
    default_args=default_args,
    description='google_as',
    schedule_interval= None, #"0 0 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)




# Analizo sentimientos y guardo en archivo
def sentiment_analysis():
    df = pd.read_parquet(f'{ruta_bucket}/filtrado/google/review.parquet', )
    Google_analisis = sentimientos_Google(df)
    # Exporta el dataframe a un archivo Parquet
    archivo = f'{ruta_bucket}/filtrado/google/Google_Analisis_Sentimiento.parquet'
    print (archivo)
    Google_analisis.to_parquet(archivo, index=False)



def google_load_bq():

    tabla = 'Google_Analisis_Sentimiento'
    table_id = f'steakhouses2.google.{tabla}'


    # Instanciar BigQuery client
    client = bigquery.Client()


    # Borrar tabla si existe
    client.delete_table(table_id, not_found_ok=True)

    # Crear tabla en BigQuery a partir de archivo
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)

    uri = f"{ruta_bucket}/filtrado/google/{tabla}.parquet"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # API request.

    load_job.result()  # Espera que se complete el trabajo.
    destination_table = client.get_table(table_id)

    # Crear registro tabla auditoria
    audit = client.get_table('steakhouses2.google.audit')
    row = bigquery.Row([datetime.now(), tabla, "Carga datos análisis", 0, destination_table.num_rows, None, None, None], [])
    client.insert_rows(audit, [row])

# Tarea Análisis de sentimientos
sentiment_analysis = PythonOperator(
    task_id="sentiment_analysis",
    python_callable=sentiment_analysis,
    dag=dag,
)


# Tarea de carga de datos YELP procesados a Bigquery
google_load_bq = PythonOperator(
    task_id="google_load_bq",
    python_callable=google_load_bq,  
    dag=dag,
)

sentiment_analysis >> google_load_bq