from Functions.api_yelp import *
from Functions.etl_yelp import sentimientos_Yelp 
import pandas as pd
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
from datetime import datetime
from google.cloud import bigquery


# En modo gratuito 500 max x dia
can_consultas = 30
ruta_bucket = "gs://bucket-steakhouses2"
business_ids = business_a_consultar(can_consultas)

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id="yelp_api",
    default_args=default_args,
    description='Yelp API',
    schedule_interval= "0 0,6,12,18 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)


## Consulta API Yelp y Guarda Resultados
def read_data_api():
    reviews = []
    for business_id in business_ids:
        reviews += API_reviews(business_id)

    df_reviews_api = pd.DataFrame(reviews)
    df_reviews_existentes = reviews_existentes()
    df_reviews_nuevas  = df_reviews_api.loc[~df_reviews_api['review_id'].isin(df_reviews_existentes['review_id'])]

    print (df_reviews_nuevas.shape[0], 'reviews nuevas')
    df_reviews_nuevas.to_parquet(f'{ruta_bucket}/filtrado/yelp/review.parquet', index=False)


## Carga Reviews nuevas desde archivo
def load_data_bq():
    archivo = f'{ruta_bucket}/filtrado/yelp/review.parquet'
    df_reviews_nuevas = pd.read_parquet(archivo)
  
    # Credenciales
    credentials, _ = google.auth.default()
    # Cliente BigQuery
    client = bigquery.Client(credentials=credentials)

    # Inserta data review a BigQuery
    table_ref = client.get_table('steakhouses2.yelp.review')
    rows_antes = table_ref.num_rows

    table_id = f'steakhouses2.yelp.review'

    if df_reviews_nuevas.shape[0]>0:
        uri = f"{ruta_bucket}/filtrado/yelp/review.parquet"
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)
        job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # API request.

        job.result()  # Espera que se complete el trabajo.

    table_ref = client.get_table('steakhouses2.yelp.review')
    rows_despues = table_ref.num_rows

    # Crear registro tabla auditoria
    ultimo = business_ids[-1] if len(business_ids)==can_consultas else ''
    audit = client.get_table('steakhouses2.yelp.audit')
    row = bigquery.Row([datetime.now(), 'review', "Carga incremental API", rows_antes, rows_despues, None, 'business_id', ultimo], [])
    client.insert_rows(audit, [row])





# Analizo sentimientos y guardo en archivo
def sentiment_analysis():
    df = pd.read_parquet(f'{ruta_bucket}/filtrado/yelp/review.parquet', )
    Yelp_analisis = sentimientos_Yelp(df)
    # Exporta el dataframe a un archivo Parquet
    archivo = f'{ruta_bucket}/filtrado/yelp/Yelp_Analisis_Sentimiento.parquet'
    print (archivo)
    Yelp_analisis.to_parquet(archivo, index=False)






##  Carga análisis de sentimientos a BQ
def load_sa_bq():

    tabla = 'Yelp_Analisis_Sentimiento'
    table_id = f'steakhouses2.yelp.{tabla}'

    # Instanciar BigQuery client
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)

    uri = f"{ruta_bucket}/filtrado/yelp/{tabla}.parquet"

    destination_table = client.get_table(table_id)
    rows_antes = destination_table.num_rows

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # API request.

    load_job.result()  # Espera que se complete el trabajo.
    destination_table = client.get_table(table_id)
    rows_despues = destination_table.num_rows
    # Crear registro tabla auditoria
    audit = client.get_table('steakhouses2.yelp.audit')
    row = bigquery.Row([datetime.now(), tabla, "Carga incremental API", rows_antes, rows_despues, None, None, None], [])
    client.insert_rows(audit, [row])






# Tarea de leer datos desde la api
read_data_api = PythonOperator(
    task_id="read_data_api",
    python_callable=read_data_api,
    dag=dag,
)


# Tarea cargar datos
load_data_bq = PythonOperator(
    task_id="load_data_bq",
    python_callable=load_data_bq,
    dag=dag,
)

# Tarea Análisis de sentimientos
sentiment_analysis = PythonOperator(
    task_id="sentiment_analysis",
    python_callable=sentiment_analysis,
    dag=dag,
)


# Tarea Cargar Análisis de sentimientos
load_sa_bq = PythonOperator(
    task_id="load_sa_bq",
    python_callable=load_sa_bq,
    dag=dag,
)



read_data_api >> load_data_bq >> sentiment_analysis >> load_sa_bq

