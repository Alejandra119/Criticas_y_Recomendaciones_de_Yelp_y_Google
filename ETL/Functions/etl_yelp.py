import pandas as pd
import re
from google.cloud import bigquery
from google.cloud import storage

# Librerías análisis de sentimientos
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import sentiment
from nltk import word_tokenize
nltk.download('vader_lexicon') # Descarga vader_lexicon para el análisis de sentimientos.
nltk.download('punkt') # Descarga punkt, un modelo de toquenización que divide el texto en palabras individuales








def existe_dataset(nombre_dataset):
    client = bigquery.Client()
    try:
        client.dataset(nombre_dataset).reload()
        return True
    except bigquery.NotFoundError:
        return False   
    



def existe_archivo(bucket_name, archivo):
    # Inicializa el cliente de GCS
    client = storage.Client()

    # Obtiene una referencia al bucket
    bucket = client.get_bucket(bucket_name)


    # Verifica si el archivo existe en la carpeta del bucket
    blob = bucket.blob(archivo)
    return blob.exists()


def remover_caracteres(text):   
    '''
    Esta función elimina todos los caracteres que no sean letras 
    '''
    
    # Patrón de expresión regular para encontrar caracteres que no sean del alfabeto
    patron = r'[^a-zA-Z\s]'  # Acepta letras minúsculas, letras mayúsculas y espacios en blanco

    # Eliminar caracteres no deseados utilizando la función sub() de re (reemplaza coincidencias)
    limpieza_text = re.sub(patron, '', text)

    return limpieza_text





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






