import streamlit as st
from functions import sentimientos_Yelp  # Asegúrate de importar la función sentimientos_Yelp desde tu archivo functions
import pandas as pd

# Crear un widget de selección para elegir un negocio
negocios = pd.read_csv('ruta_del_archivo_de_negocios.csv')  # Reemplaza 'ruta_del_archivo_de_negocios.csv' con la ruta correcta de tu archivo de negocios
negocio_seleccion = st.selectbox('Selecciona un negocio', negocios['name'])

# Función para mostrar el análisis de sentimientos
def mostrar_analisis_de_sentimientos(negocio_seleccion):
    st.write(f'Análisis de sentimientos para el negocio: {negocio_seleccion}')
    # Llama a tu función para obtener el análisis de sentimientos y muestra los resultados
    as_reviews = sentimientos_Yelp(negocio_seleccion)  # Asegúrate de que esta función tome el nombre del negocio como entrada
    st.write(as_reviews)

# Botón para iniciar el análisis de sentimientos
if st.button('Consultar'):
    mostrar_analisis_de_sentimientos(negocio_seleccion)
