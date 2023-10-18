# streamlit_app.py

import streamlit as st
import pandas as pd
import gcsfs
from functions import *

st.set_page_config(page_icon="📊", page_title="Clasificación de reseñas", layout="wide")
st.image("https://static.wixstatic.com/media/0f55e2_c8cb97b88d3d4e728390c20e7295c26e~mv2.png/v1/fill/w_148,h_78,al_c,q_85,usm_0.66_1.00_0.01,enc_auto/barritas%20logo.png", width=200)
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


