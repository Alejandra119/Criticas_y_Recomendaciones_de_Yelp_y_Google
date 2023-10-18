import requests
import pandas as pd
from datetime import datetime
import google.auth
from google.cloud import bigquery



# Clave de API de Yelp
api_key = 'oxIAsV-UOX14M1J512kB4o80N5Lv0KdArXsU8GF3kzSazG6dkV4Aqmu2bPndGLDie1Ek5ieYrqbPWkfu5u7hGEN2YMhDho16NLAKET1y9GOx-E2-rpjf-MlEOkQjZXYx'


def API_reviews(business_id):
    # Define la URL base de la API de Yelp
    base_url = f'https://api.yelp.com/v3/businesses/{business_id}/reviews'

    # Parámetros de búsqueda
    params = {}

    # Encabezados de la solicitud, incluyendo la clave de API
    headers = {
        'Authorization': f'Bearer {api_key}'
    }

    try:
        # Realiza la solicitud GET a la API de Yelp
        response = requests.get(base_url, params=params, headers=headers)
    except Exception as e:
        print(e)
        return None
    
   
    lista_reviews = []
    for r in response.json()['reviews']:
        lista_reviews.append({
            'review_id' : r['id'], 
            'user_id' : r['user']['id'], 
            'business_id' : business_id, 
            'stars' : r['rating'], 
            'useful' : 0, 
            'text' : r['text'], 
            'date' : datetime.strptime(r['time_created'], "%Y-%m-%d %H:%M:%S").date()
        }) 






    return lista_reviews


def reviews_existentes():

    # Create a credentials object
    credentials, _ = google.auth.default()
    # Create a BigQuery client
    client = bigquery.Client(credentials=credentials)

    sql = 'SELECT review_id FROM yelp.review'
    df = client.query(sql).to_dataframe()

    return df





def business_a_consultar(cantidad):

    # Create a credentials object
    credentials, _ = google.auth.default()
    # Create a BigQuery client
    #client = bigquery.Client(credentials=credentials)
    client = bigquery.Client()    



    #Ultimo negocio consultado
    sql = """
        SELECT aditional_value FROM `steakhouses2.yelp.audit` 
        WHERE table_name='review' 
            AND aditional_description='business_id' 
            AND aditional_value IS NOT NULL 
            ORDER BY date_time DESC 
            LIMIT 1
        """
    query = client.query(sql).result()       
    ultimo = query.to_dataframe().iloc[0]['aditional_value'] if query.total_rows==1 else '""'
    
    #Buscar los N negocios a consultar
    sql = f"""
        SELECT business_id FROM yelp.business
        WHERE business_id>"{ultimo}"
        ORDER BY business_id
        LIMIT {cantidad}
    """

    print(sql)

    df = client.query(sql).to_dataframe()
    lista = df['business_id'].to_list()

    print ('Negocios Consulta API', lista)

    return lista
