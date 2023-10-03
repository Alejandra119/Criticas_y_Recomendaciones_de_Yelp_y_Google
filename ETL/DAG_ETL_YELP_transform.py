import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pandas as pd
import re
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
    dag_id="etl_yelp_transform",
    default_args=default_args,
    description='ETL Yelp Transform',
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)




# # Tarea de descarga de datos
# download_data_task = BashOperator(
#     task_id="download_data",
#     bash_command="curl https://example.com/data > data.csv",
#     dag=dag,
# )


def remover_caracteres(text):   
    '''
    Esta función elimina todos los caracteres que no sean letras 
    '''
    
    # Patrón de expresión regular para encontrar caracteres que no sean del alfabeto
    patron = r'[^a-zA-Z\s]'  # Acepta letras minúsculas, letras mayúsculas y espacios en blanco

    # Eliminar caracteres no deseados utilizando la función sub() de re (reemplaza coincidencias)
    limpieza_text = re.sub(patron, '', text)

    return limpieza_text



def ETL_business(ruta, nombre_archivo):
    # Carga el archivo en Pandas
    archivo = f'{ruta}/crudo/{nombre_archivo}'
    df = pd.read_parquet(archivo)

    #Creamos una mascara
    mask = df['categories'].str.contains('steak',case = False) == True
    df = df[mask]

    #Definir las columnas que se usarán
    columnas = ['business_id', 'name', 'address', 'city', 'state', 'latitude', 'longitude', 'stars', 'review_count', 'is_open', 'categories']
    df = df[columnas]

    #Eliminar duplicados y nulos
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)

    # Guardar el DataFrame procesado 
    archivo = f'{ruta}/filtrado/business.parquet'
    df.to_parquet(archivo, index=False)





def ETL_tip(ruta, nombre_archivo):
    # Cargar el archivo en Pandas
    archivo = f'{ruta}/crudo/{nombre_archivo}'
    df = pd.read_parquet(archivo)

    # Filtrar x tipo de negocio
    archivo = f'{ruta}/filtrado/business.parquet'
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
    archivo = f'{ruta}/filtrado/tip.parquet'
    df.to_parquet(archivo, index=False)


def ETL_review(ruta, nombre_archivo):


    archivo = f'{ruta}/filtrado/business.parquet'
    business = pd.read_parquet(archivo, columns=['business_id'])
    business = business['business_id']



    # Cargar el archivo en Pandas
    filters = [('business_id', 'in', business)]
    archivo = f'{ruta}/crudo/{nombre_archivo}'
    df = pd.read_parquet(archivo, filters=filters)


    # Filtrar x tipo de negocio
    archivo = f'{ruta}/filtrado/business.parquet'
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
    archivo = f'{ruta}/filtrado/review.parquet'
    df.to_parquet(archivo, index=False)



def ETL_user(ruta, nombre_archivo):
 

 
    # Cargar el archivo en Pandas
    archivo = f'{ruta}/crudo/{nombre_archivo}'
    df = pd.read_parquet(archivo)


    # Filtramos
    columnas = ['user_id', 'review_count', 'yelping_since', 'average_stars']
    df_filtered = df[columnas]

    # Limpiar los datos
    df_filtered.dropna(inplace=True)
    df_filtered.drop_duplicates(inplace=True)

    # Transformación
    try:
        # Cambio de formato fecha
        df_filtered['yelping_since'] = pd.to_datetime(df_filtered['yelping_since'], format='%Y-%m-%d')
    except Exception as e:
        print(f"Error al convertir la columna 'yelping_since' a formato de fecha: {e}")


    # Guardar el DataFrame procesado 
    archivo = f'{ruta}/filtrado/user.parquet'
    df.to_parquet(archivo, index=False)




def ETL_checkin(ruta, nombre_archivo):

    # Cargar el archivo en Pandas
    archivo = f'{ruta}/crudo/{nombre_archivo}'
    df = pd.read_parquet(archivo)

    # Filtrar x tipo de negocio
    archivo = f'{ruta}/filtrado/business.parquet'
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
    archivo = f'{ruta}/filtrado/checkin.parquet'
    df.to_parquet(archivo, index=False)




# import google.auth

# credentials_info = {
#     "username": "flaviobovio2@gmail.com",
#     "password": "ping3328",
#     "project_id": "pghenry-dpt2 ",
# }

# #credentials = google.auth.credentials_from_service_account_info(credentials_info)
# #credentials = google.auth.default()
# print (credentials)


# # [START download_from_gdrive_to_local]
# FILE_NAME = 'https://drive.google.com/file/d/1oNBonW4G-CRt-jPTw93_2kvXLpEiJCIj'
# #FILE_NAME = 'https://drive.google.com/file/d/1r0wCAyB5h2d6heKRNtjRt8wlZTAJrYF5'
# OUTPUT_FILE = f'{ruta}/crudo/tip.parquet'
# # Configuración de las credenciales
# AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT = credentials

# download_from_gdrive_to_local = GoogleDriveToLocalOperator(
#     task_id="download_from_gdrive_to_local",
#     folder_id="",
#     file_name=FILE_NAME,
#     output_file=OUTPUT_FILE,
#     dag=dag,
# )
# # [END download_from_gdrive_to_local]




# Tarea de transformación de datos business
transform_data_business_task = PythonOperator(
    task_id="transform_data_business",
    python_callable=ETL_business,
    op_args=[ruta_bucket, "business.parquet"],    
    dag=dag,
)


# Tarea de transformación de datos tip
transform_data_tip_task = PythonOperator(
    task_id="transform_data_tip",
    python_callable=ETL_tip,
    op_args=[ruta_bucket, "tip_2009_12_31.parquet"],
    dag=dag,
)



# Tarea de transformación de datos review
transform_data_review_task = PythonOperator(
    task_id="transform_data_review",
    python_callable=ETL_review,
    op_args=[ruta_bucket, "review_2009_12_31.parquet"],
    dag=dag,
)


# Tarea de transformación de datos user
transform_data_user_task = PythonOperator(
    task_id="transform_data_user",
    python_callable=ETL_user,
    op_args=[ruta_bucket, "user_2009_12_31.parquet"],
    dag=dag,
)



# Tarea de transformación de datos checkin
transform_data_checkin_task = PythonOperator(
    task_id="transform_data_checkin",
    python_callable=ETL_checkin,
    op_args=[ruta_bucket, "checkin_2009_12_31.parquet"],
    dag=dag,
)




# Tareas de dependencia
transform_data_business_task >> transform_data_tip_task >> transform_data_review_task >> transform_data_user_task >> transform_data_checkin_task


