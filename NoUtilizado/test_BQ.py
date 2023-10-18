import google.auth
from google.cloud import bigquery

# Credenciales
credentials, _ = google.auth.default()

# Cliente BigQuery 
client = bigquery.Client(credentials=credentials)

# Consulta tabla audit
query = """
SELECT
  *
FROM
  `pghenry-dpt2.yelp.audit`
LIMIT 5
"""

# Imprime resultados
resultados = client.query(query).result()
for row in resultados:
    print (row)