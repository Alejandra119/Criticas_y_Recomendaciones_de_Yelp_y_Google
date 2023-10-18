#Librerias 
import pandas as pd
import numpy as np
import fastparquet

# Leer los datos del bucket de Cloud Storage
ruta = "gs://bucket-pghenry-dpt2"

# Manejo de excepciones
try:
    df = pd.read_parquet(ruta)
except Exception as e:
    print(f"Error al leer los datos desde Cloud Storage: {e}")
    df = None
####

# CREACIÓN DE FUNCIÓN
def etl_users(df):
    if df is None:
        return None
    
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

    # Reiniciar Indices
    df_filtered.reset_index(drop=True, inplace=True)

    return df_filtered

# Llamar a la función ETL
users_data = etl_users(df)