import pandas as pd
import warnings
import json
from collections import Counter
import pyarrow
import os
import win32com.client
warnings.filterwarnings('ignore')

def resolve_shortcut(path):
    """Resuelve un acceso directo de Windows y devuelve la ruta real."""
    shell = win32com.client.Dispatch("WScript.Shell")
    shortcut = shell.CreateShortCut(path)
    return shortcut.Targetpath


def process_directory(directory_path):
    files = os.listdir(directory_path)
    dataframes = []

    for file in files:
        if file.endswith('.json'):
            file_path = os.path.join(directory_path, file)
            try:
                df = pd.read_json(file_path, lines=True)
                dataframes.append(df)
            except Exception as e:
                print(f"Error al procesar {file_path}: {e}")

    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        return combined_df
    else:
        return None


created_dataframes = []

for root, dirs, files in os.walk(os.getcwd()):
    for file in files:
        if file.endswith('.lnk'):
            resolved_path = resolve_shortcut(os.path.join(root, file))
            if os.path.isdir(resolved_path):
                dirs.append(resolved_path)  # Agregar la ruta resuelta a la lista de directorios para ser procesada

    for directory in dirs:
        full_directory_path = os.path.join(root, directory)
        df = process_directory(full_directory_path)

        if df is not None:
            df_name = directory.replace("-", "_")
            globals()[df_name] = df
            created_dataframes.append(df_name)

print("DataFrames creados:")
for name in created_dataframes:
    print(name)

metadata_sitios['states'] = metadata_sitios['address'].str.rsplit().str[-2]

# Rellenar los valores NaN/None con una cadena vacía para evitar errores
metadata_sitios['states'] = metadata_sitios['states'].fillna('')

# Filtrar las filas donde la longitud de la columna 'state' no es igual a 2 caracteres
condition = ~metadata_sitios['states'].str.match(r'^[A-Za-z]{2}$')
metadata_sitios = metadata_sitios[~condition]

# Lista de estados deseados
estados_elegidos = ['PA', 'IL', 'DE', 'AZ', 'MO', 'NJ', 'FL', 'NV', 'CA', 'IN', 'TN', 'ID', 'LA']

# Filtrar el DataFrame
metadata_sitios = metadata_sitios[metadata_sitios['states'].isin(estados_elegidos)]

columns_to_drop = ['address', 'description', 'price', 'hours', 'MISC', 'state', 'relative_results']

metadata_sitios.drop(columns=columns_to_drop, inplace=True)

metadata_sitios = metadata_sitios[metadata_sitios['category'].apply(lambda x: any('steak house' in word.lower() or 'steak' in word.lower() or 'bistec' in word.lower() or 'beef' in word.lower() for word in x) if isinstance(x, list) else False)]

# Convertir las columnas que contienen listas a cadenas
for col in metadata_sitios.columns:
    if metadata_sitios[col].apply(type).eq(list).any():
        metadata_sitios[col] = metadata_sitios[col].astype(str)

# Eliminar filas duplicadas
metadata_sitios = metadata_sitios.drop_duplicates()

# Convertir las columnas que contienen cadenas de listas de nuevo a listas
for col in metadata_sitios.columns:
    if metadata_sitios[col].apply(type).eq(str).any():
        try:
            metadata_sitios[col] = metadata_sitios[col].apply(eval)
        except:
            pass

# Unimos los dataset de los estados selaccionados en uno solo

estados = (review_Pennsylvania, review_Illinois, review_Delaware, review_Arizona, review_Missouri, review_New_Jersey, review_Florida, review_Nevada, review_California, review_Indiana, review_Tennessee, review_Idaho, review_Louisiana)

reviews_estados = pd.concat(estados)

# Columnas a eliminar name, resp

columnas = ['name','resp']

reviews_estados.drop(columns=columnas, inplace=True)

# Convertir las columnas que contienen listas a cadenas
for col in reviews_estados.columns:
    if reviews_estados[col].apply(type).eq(list).any():
        reviews_estados[col] = reviews_estados[col].astype(str)

# Eliminar filas duplicadas
reviews_estados = reviews_estados.drop_duplicates()

# Convertir las columnas que contienen cadenas de listas de nuevo a listas
for col in reviews_estados.columns:
    if reviews_estados[col].apply(type).eq(str).any():
        try:
            reviews_estados[col] = reviews_estados[col].apply(eval)
        except:
            pass

reviews_steakhouse = pd.merge(reviews_estados, metadata_sitios[['gmap_id']], on='gmap_id', how='inner')

# Guardar reviews_estados en formato Parquet
reviews_estados.to_parquet('reviews_estados.parquet')

# Guardar metadata_sitios en formato Parquet
metadata_sitios.to_parquet('metadata_sitios.parquet')

# Guardar reviews_steakhouse en formato Parquet
reviews_steakhouse.to_parquet('reviews_steakhouse.parquet')