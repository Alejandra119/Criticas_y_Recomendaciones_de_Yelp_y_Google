# streamlit_app.py

import streamlit as st
import pandas as pd
import gcsfs

# Crea un objeto GcsFileSystem para acceder al bucket de GCP
fs = gcsfs.GCSFileSystem(project='steakhouses')


file = fs.open('bucket-steakhouses/score (1).csv')
df = pd.read_csv(file)
st.write(df)
