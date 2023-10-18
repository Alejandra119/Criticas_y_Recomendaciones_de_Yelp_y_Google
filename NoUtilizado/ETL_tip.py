import pandas as pd

def ETL_tip(ruta, nombre_archivo):
    # Cargar el archivo en Pandas
    archivo = f'{ruta}/crudo/{nombre_archivo}'
    df = pd.read_parquet(archivo)

    # Filtrar x tipo de negocio
    archivo = f'{ruta}/filtrado/business.parquet'
    business = pd.read_parquet(archivo)
    business = business['business_id']
    mask = df['business_id'].isin(business) == True
    df = df[mask]

    # Descartar columnas no significativas
    df.drop(columns='compliment_count', inplace=True)

    #Eliminar duplicados y nulos
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)

    # Guardar el DataFrame procesado 
    archivo = f'{ruta}/filtrado/tip.parquet'
    df.to_parquet(archivo, index=False)

