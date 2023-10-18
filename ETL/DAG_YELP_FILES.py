import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
from datetime import datetime
from Functions.etl_yelp import *
from airflow.providers.google.cloud.transfers.gdrive_to_local import GoogleDriveToLocalOperator



ruta_bucket = "gs://bucket-steakhouses2"

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id="yelp_files",
    default_args=default_args,
    description='yelp_files',
    schedule_interval= None, #"0 0 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)



def transform_data_business(nombre_archivo):
    # Carga el archivo en Pandas
    try:
        archivo = f'{ruta_bucket}/crudo/yelp/{nombre_archivo}'
        df = pd.read_parquet(archivo)
    except Exception as e:
        print ('Error Excepción', e)

    #Creamos una mascara
    mask = df['categories'].str.contains('steakhouses',case = False) == True
    df = df[mask]

    #Definir las columnas que se usarán
    columnas = ['business_id', 'name', 'address', 'city', 'state', 'latitude', 'longitude', 'stars', 'review_count', 'is_open', 'categories']
    df = df[columnas]

    #Eliminar duplicados y nulos
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)

    # Guardar el DataFrame procesado 
    archivo = f'{ruta_bucket}/filtrado/business.parquet'
    df.to_parquet(archivo, index=False)





def transform_data_tip(nombre_archivo):
    # Cargar el archivo en Pandas
    archivo = f'{ruta_bucket}/crudo/yelp/{nombre_archivo}'
    df = pd.read_parquet(archivo)

    # Filtrar x tipo de negocio
    archivo = f'{ruta_bucket}/filtrado/business.parquet'
    business = pd.read_parquet(archivo)
    business = business['business_id']
    mask = df['business_id'].isin(business) == True
    df = df[mask]

    # Abro el contenido con pandas
    df.drop(columns='compliment_count', inplace=True)

    #Eliminar duplicados y nulos
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)


    # Guardar el DataFrame procesado 
    archivo = f'{ruta_bucket}/filtrado/tip.parquet'
    df.to_parquet(archivo, index=False)


def transform_data_review(nombre_archivo):


    archivo = f'{ruta_bucket}/filtrado/business.parquet'
    business = pd.read_parquet(archivo, columns=['business_id'])
    business = business['business_id']



    # Cargar el archivo en Pandas
    filters = [('business_id', 'in', business)]
    archivo = f'{ruta_bucket}/crudo/yelp/{nombre_archivo}'
    df = pd.read_parquet(archivo, filters=filters)


    # Filtrar x tipo de negocio
    archivo = f'{ruta_bucket}/filtrado/business.parquet'
    business = pd.read_parquet(archivo)
    business = business['business_id']
    mask = df['business_id'].isin(business) == True
    df = df[mask]

    #Eliminar duplicados y nulos
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)


    #Eliminar las columnas que no se utilizarán; "cool" y "funny"
    df = df.drop(['cool', 'funny'], axis=1)

    #Convertir a tipo de dato datetime
    df['date'] = pd.to_datetime(df['date']) 
    # Extraer solo la parte de la fecha
    df['date'] = df['date'].dt.date


    #Cambiar los tipos de datos 
    df['stars'] = df['stars'].astype(int)
    df['text'] = df['text'].astype(str)

    # Limpiar Texto
    df['text'] = df['text'].apply(remover_caracteres)


    # Guardar el DataFrame procesado 
    archivo = f'{ruta_bucket}/filtrado/review.parquet'
    df.to_parquet(archivo, index=False)




def transform_data_checkin(nombre_archivo):

    # Cargar el archivo en Pandas
    archivo = f'{ruta_bucket}/crudo/yelp/{nombre_archivo}'
    df = pd.read_parquet(archivo)

    # Filtrar x tipo de negocio
    archivo = f'{ruta_bucket}/filtrado/business.parquet'
    business = pd.read_parquet(archivo)
    business = business['business_id']
    mask = df['business_id'].isin(business) == True
    df = df[mask]

    # Limpiar los datos de checkin.
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)

    # Dividir las fechas en una columna por comas.
    df['fechas'] = df['date'].str.split(',')

    # Duplicar las filas para cada fecha con su respectivo ID.
    df = df.explode('fechas').reset_index(drop=True)

    # Eliminar la columna original.
    df.drop('date', axis=1, inplace=True)

    # Reiniciar los índices.
    df.reset_index(drop=True, inplace=True)


    # Guardar el DataFrame procesado 
    archivo = f'{ruta_bucket}/filtrado/checkin.parquet'
    df.to_parquet(archivo, index=False)



def transform_data_user(nombre_archivo):
 
    # Cargar el archivo en Pandas
    archivo = f'{ruta_bucket}/crudo/yelp/{nombre_archivo}'
    df = pd.read_parquet(archivo)


    # Filtramos
    columnas = ['user_id', 'review_count', 'yelping_since', 'average_stars']
    df = df[columnas]

    # Limpiar los datos
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)

    # Transformación
    try:
        # Cambio de formato fecha
        df['yelping_since'] = pd.to_datetime(df['yelping_since'], format='%Y-%m-%d')
    except Exception as e:
        print(f"Error al convertir la columna 'yelping_since' a formato de fecha: {e}")

    # Guardar el DataFrame procesado 
    archivo = f'{ruta_bucket}/filtrado/user.parquet'
    df.to_parquet(archivo, index=False)





def load_data_bq():
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



















# Tarea de transformación de datos business
transform_data_business = PythonOperator(
    task_id="transform_data_business",
    python_callable=transform_data_business,
    op_args=["business.parquet"],    
    dag=dag,
)


# Tarea de transformación de datos tip
transform_data_tip = PythonOperator(
    task_id="transform_data_tip",
    python_callable=transform_data_tip,
    op_args=["tip.parquet"],
    dag=dag,
)



# Tarea de transformación de datos review
transform_data_review = PythonOperator(
    task_id="transform_data_review",
    python_callable=transform_data_review,
    op_args=["review.parquet"],
    dag=dag,
)


# Tarea de transformación de datos user
transform_data_user = PythonOperator(
    task_id="transform_data_user",
    python_callable=transform_data_user,
    op_args=["user.parquet"],
    dag=dag,
)



# Tarea de transformación de datos checkin
transform_data_checkin = PythonOperator(
    task_id="transform_data_checkin",
    python_callable=transform_data_checkin,
    op_args=["checkin.parquet"],
    dag=dag,
)

# Tarea de carga de datos YELP procesados a Bigquery
load_data_bq = PythonOperator(
    task_id="load_data_bq",
    python_callable=load_data_bq,  
    dag=dag,
)



# Tareas de dependencia
transform_data_business >> transform_data_tip >> transform_data_review >> transform_data_user >> transform_data_checkin >> load_data_bq


