from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import datetime
from DAG_ETL_YELP_transform import *


ruta_bucket = "gs://bucket-steakhouses2"

dag = DAG(
    dag_id="bucket_monitor",
    schedule_interval=None,
    start_date=datetime.datetime.now(),
)

# Monitorear creación en bucket
list_gcs_objects_task = GCSListObjectsOperator(
    task_id="list_gcs_objects",
    bucket=ruta_bucket,
    prefix="crudo/yelp",
    delimiter="/",
    dag=dag,
)


# Disparar el DAG_YELP_FILES
trigger_yelp = TriggerDagRunOperator(
    task_id='trigger_yelp',
    trigger_dag_id='DAG_YELP_FILES.py',
    trigger_dag_name='dag_yelp_files',
    dag=dag
)



list_gcs_objects_task >> trigger_yelp

