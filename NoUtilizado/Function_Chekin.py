# Importamos la librería a usar
import pandas as pd

# Leer los datos del bucket de Cloud Storage
ruta = "gs://bucket-pghenry-dpt2"

import pandas as pd

# Leer los datos del bucket de Cloud Storage
ruta = "gs://bucket-pghenry-dpt2"


def etl_checkin_id(df):
    """ Archivo Business"""

    # Comprobar si el DataFrame es nulo.
    if df is None:
        return None

    # Obtener los Id's de los restaurantes de la categoría "steakhouses".
    mask_id = df['categories'].str.contains('steakhouses', case=False) == True
    steakhouses = df[mask_id] [['business_id']]

    # Limpiar los datos.
    steakhouses.dropna(inplace=True)
    steakhouses.drop_duplicates(inplace=True)

    """ Archivo Checkin"""
    # Filtrar los datos de checkin por los Id's de restaurantes de la categoría "steakhouses".
    df_checkin = df[df['business_id'].isin(steakhouses['business_id'])]

    # Limpiar los datos de checkin.
    df_checkin.dropna(inplace=True)
    df_checkin.drop_duplicates(inplace=True)

    # Dividir las fechas en una columna por comas.
    df_checkin['fechas'] = df_checkin['date'].str.split(',')

    # Duplicar las filas para cada fecha con su respectivo ID.
    df_checkin = df_checkin.explode('fechas').reset_index(drop=True)

    # Eliminar la columna original.
    df_checkin.drop('date', axis=1, inplace=True)

    # Reiniciar los índices.
    df_checkin.reset_index(drop=True, inplace=True)

    return df_checkin

# Llamar a la función ETL
checkin_data = etl_checkin_id(df)

