
#Funciones para el Pipeline

#Importar librerias
import glob
import pandas as pd
import re

ruta = "gs://bucket-pghenry-dpt2"
df_review = pd.read_parquet(ruta)



def filtro_palabras(df_review):
    
    '''
    Esta función filtra el dataset por palabras escritas en la reseña
    '''
    
    palabras = ["meat", "roast", "beef", "steak"]
    mascara = df_review['text'].str.contains('|'.join(palabras))
    df_final = df_review[mascara]
    return df_final



df_review = filtro_palabras(df_review)




def eliminar_columnas(df_final):
    
    '''
    Esta función elimina las columnas innecesarias
    '''
    
    df_final = df_final.drop(['cool', 'funny'], axis=1)
    return df_final


df_review = eliminar_columnas(df_review)




def fecha(df_final):
    
    '''
    Esta función deja en la columna solo la fecha con formato YYYY-MM-DD
    '''
    
    df_final['date'] = pd.to_datetime(df_final['date']) #Convertir a tipo de dato datetime

    # Extraer solo la parte de la fecha
    df_final['date'] = df_final['date'].dt.date
    return df_final
  
  
  
df_review = fecha(df_review) 






def cambio_tipo_dato(df_final):
    
    '''
    Esta función cambia los tipos de datos
    ''' 
    
    df_final['stars'] = df_final['stars'].astype(int)
    df_final['text'] = df_final['text'].astype(str)
    return df_final



df_review = cambio_tipo_dato(df_review)





def remover_caracteres(text):
    
    '''
    Esta función elimina todos los caracteres que no sean letras 
    '''
    
    # Patrón de expresión regular para encontrar caracteres que no sean del alfabeto
    patron = r'[^a-zA-Z\s]'  # Acepta letras minúsculas, letras mayúsculas y espacios en blanco

    # Eliminar caracteres no deseados utilizando la función sub() de re (reemplaza coincidencias)
    limpieza_text = re.sub(patron, '', text)

    return limpieza_text




df_review['text'] = df_review['text'].apply(remover_caracteres)