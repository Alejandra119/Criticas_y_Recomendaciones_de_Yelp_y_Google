import pandas as pd
import re


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
    try:
        archivo = f'{ruta}/crudo/yelp/{nombre_archivo}'
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
    archivo = f'{ruta}/filtrado/business.parquet'
    df.to_parquet(archivo, index=False)





def ETL_tip(ruta, nombre_archivo):
    # Cargar el archivo en Pandas
    archivo = f'{ruta}/crudo/yelp/{nombre_archivo}'
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
    archivo = f'{ruta}/crudo/yelp/{nombre_archivo}'
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




def ETL_checkin(ruta, nombre_archivo):

    # Cargar el archivo en Pandas
    archivo = f'{ruta}/crudo/yelp/{nombre_archivo}'
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



def ETL_user(ruta, nombre_archivo):
 
    # Cargar el archivo en Pandas
    archivo = f'{ruta}/crudo/yelp/{nombre_archivo}'
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
    archivo = f'{ruta}/filtrado/user.parquet'
    df.to_parquet(archivo, index=False)









