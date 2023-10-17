import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import re
#from google.cloud import bigquery
from datetime import datetime, timedelta
import googlemaps
import pandas as pd

api_key = ''

# Inicializar el cliente de Google Maps
gmaps = googlemaps.Client(key=api_key)

# Definir argumentos predeterminados del DAG
default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Inicializar el DAG
dag = DAG(
    dag_id="API_Google",
    default_args=default_args,
    description='API_Google',
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(days=1),
)

# Funciones que serán llamadas por los operadores de Python
def extract_places_data(metadata_sitios, **kwargs):
    data = {
        'name': [],
        'gmap_id': [],
        'latitude': [],
        'longitude': [],
        'category': [],
        'avg_rating': [],
        'num_of_reviews': [],
        'url': [],
        'address': [],
        'states': []  # Puedes necesitar otra API o lógica para determinar el estado basado en las coordenadas
    }

    # Iterar sobre las filas del DataFrame y realizar una solicitud de API para cada punto
    for _, row in metadata_sitios.iterrows():
        location = f"{row['latitude']},{row['longitude']}"
        keyword = 'steak house'

        # Realizar la búsqueda de lugares
        places_result = gmaps.places_nearby(location=location, keyword=keyword, radius=5000)

        # Extraer los resultados
        places = places_result['results']

        # Iterar sobre los lugares y extraer los datos
        for place in places:
            data['name'].append(place['name'])
            data['gmap_id'].append(place['place_id'])
            data['latitude'].append(place['geometry']['location']['lat'])
            data['longitude'].append(place['geometry']['location']['lng'])
            data['category'].append(place['types'])
            data['avg_rating'].append(place.get('rating', None))
            data['num_of_reviews'].append(place.get('user_ratings_total', None))
            data['url'].append(f"https://www.google.com/maps/place/?q=place_id:{place['place_id']}")
            data['states'].append(None)  # Necesitarás lógica adicional para determinar el estado

    # Convertir los datos en un DataFrame
    places_df = pd.DataFrame(data)

    return (places_df)

def extract_reviews_data(**kwargs):
    data = {
        'user_id': [],
        'name': [],
        'time': [],
        'rating': [],
        'text': [],
        'pics': [],
        'resp': [],
        'gmap_id': []
    }

    # Iterar sobre los IDs de Google Maps en tu DataFrame y realizar una solicitud de API para cada lugar
    for gmap_id in places_df['gmap_id']:
        # Obtener los detalles del lugar
        place_details = gmaps.place(place_id=gmap_id, fields=['review'])

        # Verificar si hay reseñas disponibles
        if 'reviews' in place_details['result']:
            # Iterar sobre las reseñas y extraer los datos
            for review in place_details['result']['reviews']:
                data['user_id'].append(review.get('author_url', None))  # Utilizar None si 'author_url' no está presente
                data['name'].append(review.get('author_name', None))  # Utilizar None si 'author_name' no está presente
                data['time'].append(review.get('relative_time_description',
                                               None))  # Utilizar None si 'relative_time_description' no está presente
                data['rating'].append(review.get('rating', None))  # Utilizar None si 'rating' no está presente
                data['text'].append(review.get('text', None))  # Utilizar None si 'text' no está presente
                data['pics'].append(None)  # La API no proporciona imágenes en las reseñas
                data['resp'].append(None)  # La API no proporciona respuestas a las reseñas
                data['gmap_id'].append(gmap_id)

    # Convertir los datos en un DataFrame
    reviews_df = pd.DataFrame(data)
    return (reviews_df)

def save_data_to_parquet(**kwargs):
    # Guardar  en formato Parquet
    reviews_df.to_parquet('gs://bucket-steakhouses2/filtrado/google/API_reviews_estados.parquet')

    places_df.to_parquet('gs://bucket-steakhouses2/filtrado/google/API_metadata_sitios.parquet')

# Definir tareas utilizando operadores
t1 = PythonOperator(
    task_id='extract_places_data',
    python_callable=extract_places_data,
    op_args=[metadata_sitios],
    provide_context=True,
    dag=dag,
)


t2 = PythonOperator(
    task_id='extract_reviews_data',
    python_callable=extract_reviews_data,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='save_data_to_parquet',
    python_callable=save_data_to_parquet,
    provide_context=True,
    dag=dag,
)

# Definir las dependencias entre tareas
t1 >> t2 >> t3
