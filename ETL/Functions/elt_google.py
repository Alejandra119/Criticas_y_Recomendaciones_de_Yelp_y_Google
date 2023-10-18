# Importamos las librerías a usar
import json
from google.cloud import storage


import pandas as pd
import google.auth
from google.cloud import bigquery
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import sentiment
from nltk import word_tokenize
nltk.download('vader_lexicon') # Descarga vader_lexicon para el análisis de sentimientos.
nltk.download('punkt') # Descarga punkt, un modelo de toquenización que divide el texto en palabras individuales




# Tarea para leer todos los archivos JSON
def process_all_json_files(input_path):
    """Lee todos los archivos JSON en un bucket de GCS.

    Args:
        input_path (str): Ruta al bucket y directorio que contiene los archivos JSON.

    Returns:
        list[dict]: Una lista de diccionarios JSON.
    """
    # Dividir la ruta de GCS en bucket y prefix
    bucket_name, prefix = input_path.replace("gs://", "").split("/", 1)

    # Crear un cliente de GCS
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    # Listar los blobs en el bucket usando el prefijo
    blobs = bucket.list_blobs(prefix=prefix)

    json_data = []
    for blob in blobs:
        # Leer el blob como una cadena y cargar el JSON
        json_str = blob.download_as_text()
        try:
            json_data.append(json.loads(json_str))
        except json.JSONDecodeError:
            print(f"Error decoding JSON from blob {blob.name}. Blob is empty or not valid JSON.")
            # O puedes usar logging en lugar de print para registrar el error
            # logging.error(f"Error decoding JSON from blob {blob.name}. Blob is empty or not valid JSON.")

    return json_data
# Tarea para juntar todos los datos JSON en un solo DataFrame
def merge_json_data(input_path, output_path):
    """Junta todos los datos JSON en un solo DataFrame.

    Args:
        input_path (str): Ruta al archivo que contiene los datos JSON.
        output_path (str): Ruta al archivo Parquet de salida.

    Returns:
        str: Ruta al archivo Parquet de salida.
    """
    # Leer los datos JSON desde input_path
    with open(input_path, 'r') as f:
        json_data = json.load(f)


    # ... [Resto de la lógica de la función]
    df = pd.DataFrame()
    #




    # Guardar el DataFrame como un archivo Parquet en output_path
    df.to_parquet(output_path, index=False)

    return output_path


# Tarea para procesar los datos JSON
def process_json_data(df):
    """Procesa los datos JSON en un DataFrame.

    Args:
        df (pandas.DataFrame): Un DataFrame que contiene los datos JSON.

    Returns:
        pandas.DataFrame: Un DataFrame que contiene los datos JSON procesados.
    """

    # Aquí puedes agregar el código para procesar los datos JSON. Por ejemplo, puedes eliminar columnas, transformar datos, etc.

    return df

# Tarea para escribir el DataFrame a un archivo Parquet
def write_dataframe_to_parquet(df, output_path):
    """Escribe un DataFrame a un archivo Parquet.

    Args:
        df (pandas.DataFrame): Un DataFrame que contiene los datos.
        output_path (str): Ruta al archivo Parquet.
    """

    df.to_parquet(output_path, index=False)

def check_files_exist():
    """Comprueba si los archivos JSON están disponibles.

    Returns:
        bool: True si los archivos JSON están disponibles, False si no.
    """
    # Obtener la lista de archivos JSON
    blob_names = ["gs://bucket-steakhouses2/crudo/google/reviews-estados/" + blob.name for blob in bucket.list_blobs(prefix="")]

    # Verificar si los archivos JSON existen
    for blob_name in blob_names:


        # if not bucket.get_blob(blob_name).exists():
        #     return False



        pass

    return True



















def sentimientos_Google (google_as):
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
    google_as['sentiment'] = google_as.rating.apply(to_sentiment)

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
    google_as['temas'] = google_as['text'].apply(classify_text)



    analizador = SentimentIntensityAnalyzer()
    def Puntaje_Sentimiento(text):
        tokens = word_tokenize(text)  # Tokenizar el texto
        scores = analizador.polarity_scores(text)  # Obtener los puntajes de sentimiento
        return scores['compound']  # Retornar el puntaje compuesto

    google_as['score_sentimientos'] = google_as['text'].apply(Puntaje_Sentimiento)

    google_as['categorizacion'] = 0  # Inicializamos con 0 por defecto

    # Aplicamos las condiciones para actualizar los valores en la nueva columna
    google_as.loc[(google_as['score_sentimientos'] > -1) & (google_as['score_sentimientos'] < 0), 'categorizacion'] = 0
    google_as.loc[google_as['score_sentimientos'] == 0, 'categorizacion'] = 1
    google_as.loc[(google_as['score_sentimientos'] >= 0.1) & (google_as['score_sentimientos'] <= 1), 'categorizacion'] = 2


    # Supongamos que tienes un DataFrame llamado 'google_as'
    google_as['nuevos_scores'] = google_as['score_sentimientos'] + 1


    return google_as
