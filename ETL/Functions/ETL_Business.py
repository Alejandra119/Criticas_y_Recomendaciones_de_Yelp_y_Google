# Librerias
import pandas as pd
import numpy as np
import fastparquet

# Leer los datos del bucket de Cloud Storage
ruta = "gs://bucket-pghenry-dpt2"

#Manejo de excepciones
try:
    df = pd.read_parquet(ruta)
except Exception as e:
    print(f"Error al leer los datos desde Cloud Storage: {e}")
    df = None

#############
#CREACIÓN DE FUNCIÓN

def etl_business(df):
    if df is None:
        return None

    # Creamos una mascara
    mask = df['categories'].str.contains('Steakhouses', case=False)

    # Filtramos
    steakhouses_df = df.loc[mask, ['business_id', 'name', 'address', 'city', 'latitude', 
                                   'longitude', 'stars', 'review_count', 'is_open', 'categories']]

    # Limpiar los datos
    steakhouses_df.dropna(inplace=True)
    steakhouses_df.drop_duplicates(inplace=True)

    # Transformación
    def reorganize_categories(x):
        return ', '.join(sorted(x.split(', '), key=lambda s: s != 'Steakhouses'))

    # Aplicar la función a la columna 'categories'
    steakhouses_df['categories'] = steakhouses_df['categories'].apply(reorganize_categories)

    # Separar las categorías en una nueva columna 'Category' y las demás en 'Subcategory'
    steakhouses_df['category'] = steakhouses_df['categories'].str.split(', ').str[0]
    steakhouses_df['subcategory'] = steakhouses_df['categories'].str.split(', ').str[1:].apply(', '.join)

    # Eliminar la columna original 'categories'
    steakhouses_df.drop('categories', axis=1, inplace=True)

    # Reiniciar Indices
    steakhouses_df.reset_index(drop=True, inplace=True)

    return steakhouses_df

# Llamar a la función ETL
business_data = etl_business(df)




