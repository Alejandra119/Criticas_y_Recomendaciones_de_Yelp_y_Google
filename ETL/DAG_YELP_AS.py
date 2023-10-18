import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
from datetime import datetime
import pandas as pd
from Functions.etl_yelp import *
from google.cloud import bigquery



ruta_bucket = "gs://bucket-steakhouses2"

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id="yelp_as",
    default_args=default_args,
    description='yelp_as',
    schedule_interval= None, #"0 0 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)



# Analizo sentimientos y guardo en archivo
def sentiment_analysis():
    df = pd.read_parquet(f'{ruta_bucket}/filtrado/yelp/review.parquet', )
    Yelp_analisis = sentimientos_Yelp(df)
    # Exporta el dataframe a un archivo Parquet
    archivo = f'{ruta_bucket}/filtrado/yelp/Yelp_Analisis_Sentimiento.parquet'
    print (archivo)
    Yelp_analisis.to_parquet(archivo, index=False)








def yelp_load_bq():

    tabla = 'Yelp_Analisis_Sentimiento'
    table_id = f'steakhouses2.yelp.{tabla}'


    # Instanciar BigQuery client
    client = bigquery.Client()

    # Crear tabla en BigQuery a partir de archivo
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)

    uri = f"{ruta_bucket}/filtrado/yelp/{tabla}.parquet"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # API request.

    load_job.result()  # Espera que se complete el trabajo.
    destination_table = client.get_table(table_id)

    # Crear registro tabla auditoria
    audit = client.get_table('steakhouses2.yelp.audit')
    row = bigquery.Row([datetime.now(), tabla, "Carga datos análisis", 0, destination_table.num_rows, None, None, None], [])
    client.insert_rows(audit, [row])



# Tarea Análisis de sentimientos
sentiment_analysis = PythonOperator(
    task_id="sentiment_analysis",
    python_callable=sentiment_analysis,
    dag=dag,
)


# Tarea de carga de datos YELP procesados a Bigquery
yelp_load_bq = PythonOperator(
    task_id="yelp_load_bq",
    python_callable=yelp_load_bq,  
    dag=dag,
)

sentiment_analysis >> yelp_load_bq