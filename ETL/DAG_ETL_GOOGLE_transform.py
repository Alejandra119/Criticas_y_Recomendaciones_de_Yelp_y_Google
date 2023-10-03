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
    dag_id="etl_google_transform",
    default_args=default_args,
    description='ETL Google Transform',
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)



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


    # Creamos una mascara
    mask = df['categories'].str.contains('Steakhouses', case=False)

    # Filtramos
    df = df.loc[mask, ['business_id', 'name', 'address', 'city', 'latitude', 
                                   'longitude', 'stars', 'review_count', 'is_open', 'categories']]


    #Eliminar duplicados y nulos
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)

    # Transformación
    def reorganize_categories(x):
        return ', '.join(sorted(x.split(', '), key=lambda s: s != 'Steakhouses'))

    # Aplicar la función a la columna 'categories'
    df['categories'] = df['categories'].apply(reorganize_categories)

    # Separar las categorías en una nueva columna 'Category' y las demás en 'Subcategory'
    df['category'] = df['categories'].str.split(', ').str[0]
    df['subcategory'] = df['categories'].str.split(', ').str[1:].apply(', '.join)

    # Eliminar la columna original 'categories'
    df.drop('categories', axis=1, inplace=True)

    # Guardar el DataFrame procesado 
    archivo = f'{ruta}/filtrado/business.parquet'
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









# Tarea de transformación de datos business
transform_data_business_task = PythonOperator(
    task_id="transform_data_business",
    python_callable=ETL_business,
    op_args=[ruta_bucket, "business.parquet"],    
    dag=dag,
)




# Tarea de transformación de datos review
transform_data_review_task = PythonOperator(
    task_id="transform_data_review",
    python_callable=ETL_review,
    op_args=[ruta_bucket, "review_2009_12_31.parquet"],
    dag=dag,
)




# Tareas de dependencia
transform_data_business_task >>  transform_data_review_task 
