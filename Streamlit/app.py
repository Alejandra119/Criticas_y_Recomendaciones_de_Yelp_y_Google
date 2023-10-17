# streamlit_app.py

import streamlit as st
import pandas as pd
import gcsfs
from functions import *


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


