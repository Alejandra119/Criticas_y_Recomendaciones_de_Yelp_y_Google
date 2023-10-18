# Importamos las librerías a usar
import pandas as pd
from datetime import datetime
import requests
import google.auth
from google.cloud import bigquery
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import sentiment
from nltk import word_tokenize
nltk.download('vader_lexicon') # Descarga vader_lexicon para el análisis de sentimientos.
nltk.download('punkt') # Descarga punkt, un modelo de toquenización que divide el texto en palabras individuales





def negocios():

    # # # Credenciales
    # # credentials, _ = google.auth.default()
    # # # Cliente BigQuery 
    # # client = bigquery.Client(credentials=credentials)

    # client = bigquery.Client()
    # # Consulta negocios
    # sql = """
    #     SELECT business_id, name FROM `steakhouses2.yelp.business` 
    #         ORDER BY name
    #     """
    # query = client.query(sql).result()
    # df = query.to_dataframe()

    # return df

    df = pd.read_parquet('business.parquet', columns=['business_id', 'name'])
    return df





def API_reviews(business_id):
    # Clave de API de Yelp
    api_key = 'oxIAsV-UOX14M1J512kB4o80N5Lv0KdArXsU8GF3kzSazG6dkV4Aqmu2bPndGLDie1Ek5ieYrqbPWkfu5u7hGEN2YMhDho16NLAKET1y9GOx-E2-rpjf-MlEOkQjZXYx'

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


    return pd.DataFrame(lista_reviews)


def sentimientos_Yelp (yelp_as):
    # Convertir las estrellas en score
    def to_sentiment(rating):
        
        rating = int(rating)
        
        # Convert to class
        if rating <= 2:
            return 0
        elif rating == 3:
            return 1
        else:
            return 2

    # Apply to the dataset 
    yelp_as['sentiment'] = yelp_as.stars.apply(to_sentiment)

    # Función para clasificar los textos
    def classify_text(text):
        if 'food' in text.lower():
            return 'comida'
        elif 'service' in text.lower():
            return 'servicio'
        elif 'ambience' in text.lower():
            return 'ambiente'
        return 'otro'

    # Aplica la función a la columna 'text' y crea una nueva columna 'temas'
    yelp_as['temas'] = yelp_as['text'].apply(classify_text)



    analizador = SentimentIntensityAnalyzer()
    def Puntaje_Sentimiento(text):
        tokens = word_tokenize(text)  # Tokenizar el texto
        scores = analizador.polarity_scores(text)  # Obtener los puntajes de sentimiento
        return scores['compound']  # Retornar el puntaje compuesto

    yelp_as['score_sentimientos'] = yelp_as['text'].apply(Puntaje_Sentimiento)

    yelp_as['categorizacion'] = 0  # Inicializamos con 0 por defecto

    # Aplicamos las condiciones para actualizar los valores en la nueva columna
    yelp_as.loc[(yelp_as['score_sentimientos'] > -1) & (yelp_as['score_sentimientos'] < 0), 'categorizacion'] = 0
    yelp_as.loc[yelp_as['score_sentimientos'] == 0, 'categorizacion'] = 1
    yelp_as.loc[(yelp_as['score_sentimientos'] >= 0.1) & (yelp_as['score_sentimientos'] <= 1), 'categorizacion'] = 2


    # Supongamos que tienes un DataFrame llamado 'yelp_as'
    yelp_as['nuevos_scores'] = yelp_as['score_sentimientos'] + 1

    return yelp_as