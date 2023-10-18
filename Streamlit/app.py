# streamlit_app.py

import streamlit as st
import pandas as pd
import gcsfs
from functions import *

st.set_page_config(page_icon="📊", page_title="Clasificación de reseñas", layout="wide")
st.image("https://github.com/Alejandra119/Criticas_y_Recomendaciones_de_Yelp_y_Google/raw/main/images/1695228347590.jpg", width=200)
st.title("Clasificación de reseñas a partir de la API de Yelp")
c29, c30, c31 = st.columns([1, 6, 1]) # 3 columnas: 10%, 60%, 10%


# # Crea un objeto GcsFileSystem para acceder al bucket de GCP
# fs = gcsfs.GCSFileSystem(project='steakhouses2')
# file = fs.open('bucket-steakhouses2/score (1).csv')
# df = pd.read_csv(file)
# st.write(df)

def mostrar_as(negocio):
    id = negocios.loc[negocios['name']==negocio]['business_id'].iloc[0]
    reviews = API_reviews(id)
    as_reviews =  sentimientos_Yelp (reviews)
    st.write(as_reviews)



negocios = negocios()
negocio_seleccion = st.sidebar.selectbox('**Negocio**', options=negocios['name'])
boton_consultar = st.sidebar.button('Consultar', on_click=mostrar_as(negocio_seleccion))


