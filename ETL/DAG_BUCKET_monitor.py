from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.trigger_dag_run_operator import TriggerDagRunOperator



import datetime
dag = DAG(
    dag_id="check_gcs_objects",
    schedule_interval=None,
    start_date=datetime.datetime.now(),
)

# Monitorear creación en bucket
list_gcs_objects_task = GCSListObjectsOperator(
    task_id="list_gcs_objects",
    bucket="bucket-pghenry-dpt2",
    prefix="crudo",
    delimiter="/",
    dag=dag,
)


# Disparar el DAG de otro archivo
task = TriggerDagRunOperator(
    task_id='trigger_dag',
    trigger_dag_id='DAG_ETL_YELP_transform.py',
    trigger_dag_name='etl_yelp_transform',
    dag=dag
)













list_gcs_objects_task >> print_task