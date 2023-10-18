<h1 align="center">ANALYTICAL INSIGHTS CO. </h1>
<p align="center">
  <img src="images/1695228347590.jpg" alt="Analitycal Insights C.O" width="300">
</p>

<h1 align="center">¿Quienes somos?</h1>
<p align="justify">
<i> Somos una empresa especializada en el análisis profundo de datos que ofrece asistencia para la toma de decisiones. 

<hr>
<h1 align="center">PROYECTO GOOGLE MAPS + YELP </h1>
<hr>

<p align="center">
  <img src="images/Yelpp.png" alt="Yelp" width="150"> <img src="images/Maps.png" alt="Maps" width="150">
</p>

<h1 align="center">Alcance del Proyecto</h1>

<p align="justify"> 
Este proyecto consiste en analizar el mercado de Steak House en algunos sectores de Estados Unidos, mediante la extracción de datos de las fuentes Google Maps y Yelp. 

<h1 align="center">Planteamiento del Problema</h1>
<p align="justify"> 
Nuestro cliente es un emprendedor del rubro "Steakhouses" en los Estados Unidos. Nuestra misión es potenciar la visibilidad y el impacto de su negocio, tanto entre los comensales que disfrutan de su experiencia al tomar decisiones basadas en las reseñas que descubren en la plataforma de Yelp y Google.

<h1 align="center">Objetivos del Proyecto</h1>

1) Estructurar una base de datos para consumo de analisis y modelamiento

2) Realizar un Modelo de Machine Learning de analisis de sentimientos sobre las críticas, reviews y opiniones de los usuarios
   
3) Elaborar reporte dinámico que permita analizar las tendencias del consumidor de Steakhouses, identificando riesgos y oportunidades a traves del seguimiento

<h1 align="center">KPI's del Proyecto</h1>

- Calificación media del restaurante en función a la media de calificación de competidores directos por localidad.
- Media de percepción del restaurante en base a las reseñas categorizadas.
- Tasa de clientes insatisfechos del mes.
- Tasa de visitas reseñadas.
- Tasa de reservas futuras. 


<h1 align="center">Solución del proyecto</h1>

La solución propuesta consta de tres fases que combina un enfoque data-driven y la creación de un modelo de análisis de sentimiento NLP para abordar la creciente competencia en la industria de los restaurantes de Steak House. Al implementar este sistema, los restaurantes podrán mejorar la satisfacción del cliente, tomar decisiones basadas en datos y mantener su posición competitiva en el mercado.


<h1 align="center">Nuestro equipo </h1>
<p align="center">
  <img src="images/Team.png" alt="Team">
</p>

  - [Mauricio Bernal](https://www.linkedin.com/in/mauricio-bernal-portocarrero/) - Product Manager
  - [Sergio Lopez]() - Program Manager
  - [Flavio Bovio](https://www.linkedin.com/in/flavio-bovio/) - Data Engineer
  - [Betzaida Loyo](https://www.linkedin.com/in/betzaida-loyo-2342821b8/) - Data Analyst
  - [Alejandra Salas](https://www.linkedin.com/in/alejandra-lizeth-salas-talavera/) - Data Scientist

<h1 align="center">Tecnologías usadas</h1>

![Visual Studio Code](https://img.shields.io/badge/Visual%20Studio%20Code-0078d7.svg?style=for-the-badge&logo=visual-studio-code&logoColor=white)
![Google Cloud](https://img.shields.io/badge/GoogleCloud-%234285F4.svg?style=for-the-badge&logo=google-cloud&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/numpy-%23013243.svg?style=for-the-badge&logo=numpy&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-%23F7931E.svg?style=for-the-badge&logo=scikit-learn&logoColor=white)
![Matplotlib](https://img.shields.io/badge/Matplotlib-%23ffffff.svg?style=for-the-badge&logo=Matplotlib&logoColor=black)
![Power Bi](https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Jupyter Notebook](https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=for-the-badge&logo=jupyter&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)
![Markdown](https://img.shields.io/badge/markdown-%23000000.svg?style=for-the-badge&logo=markdown&logoColor=white)
![Streamlit](https://img.shields.io/badge/streamlit-%23000000.svg?style=for-the-badge&logo=streamlit&logoColor=white)
![Wix](https://img.shields.io/badge/wix-%23000000.svg?style=for-the-badge&logo=wix&logoColor=white)

<h1 align="center">Desarrollo del Proyecto</h1>

## Analisis preliminares - EDA´s (Exploratory Data Analysis)
Se ha realizado un [EDA_Preliminar](https://github.com/Alejandra119/Criticas_y_Recomendaciones_de_Yelp_y_Google/tree/main/Exploracion_Inicial) que es la exploración de las diferentes fuentes de datos de Google Maps y Yelp. El propósito de estos analisis preliminares consiste en determinar qué datos serán funcionlaes para los objetivos propuestos anteriormente con el fin de hacer una limpieza y crear la arquitectura del Data Warehouse para la ejecución del proyecto. 

## Diagrama del proyecto
![Pipeline](https://github.com/Alejandra119/Criticas_y_Recomendaciones_de_Yelp_y_Google/blob/main/images/Diagrama3.jpg)

## Arquitectura del proyecto
- Creación: Se utiliza una base de datos en Big Query que proporciona la escalabilidad, buen costo, funcionalidad y rendimiento.
- Automatización: Se utiliza Cloud Composer que proporciona escalabilidad, reutilización y seguimiento.
- Carga y Análisis: El volúmen crudo fue de 35 gb, carga inicial 85 minutos, se utilizan los datos de todos los estados y carga incremental de la API de Yelp.

## Modelos de Machine Learning:
- [Análisis de Sentimientos:](https://analisis-de-sentimientos-qxkp.onrender.com/) Se clasifican los comentarios en bueno, neutro y malo, así como la intensidad de este para las empresas.
![AS](https://github.com/Alejandra119/Criticas_y_Recomendaciones_de_Yelp_y_Google/blob/main/images/AS.jpg)

- [Modelo de Recomendación:](https://sistemarecomendacion.streamlit.app/) Se recomienda a los usuarios restaurantes similares a sus comportamientos de consumo.
![MR](https://github.com/Alejandra119/Criticas_y_Recomendaciones_de_Yelp_y_Google/blob/main/images/SR.png)

## Dashboard
Se realizó un [Dashboard](https://app.powerbi.com/view?r=eyJrIjoiZjgyZjZmNjAtN2ZjNy00OGQzLWE5OWMtYjY1YTgzYTY1YWJhIiwidCI6ImNjNjNkZjFhLTZiYzktNGQ3My1iNzM0LWEyOTRkMzI1MzE4NyIsImMiOjR9) interactivo mostrando indicadores claves para las empresas así como los KPI's anteriormente mencionados para una mejora en la toma de decisiones en la implementación de estrategias.
![Dashboard](https://github.com/Alejandra119/Criticas_y_Recomendaciones_de_Yelp_y_Google/blob/main/images/Dashboard.jpg)


<h1 align="center">Documentos adicionales</h1>

- [Planificación del Proyecto](https://github.com/users/Alejandra119/projects/2/views/1)
- [Dcoumentación](https://docs.google.com/document/d/1UedDALNPgtT3v3JtRGXdhmiAqg43AttiR6wwsCfD57k/edit?usp=sharing)
- [Página web](https://mauriciobernalp6.wixsite.com/analytical-insights)






