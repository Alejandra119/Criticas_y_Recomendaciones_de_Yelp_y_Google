import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
from Functions.etl_yelp import *
#from airflow.operators.http import HttpOperator
#from airflow.providers.google.cloud.operators.storage import ReadFromGoogleDriveFile
from airflow.providers.google.cloud.transfers.gdrive_to_local import GoogleDriveToLocalOperator



ruta_bucket = "gs://bucket-steakhouses2"

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id="etl_yelp_transform",
    default_args=default_args,
    description='ETL Yelp Transform',
    schedule_interval= None, #"0 0 * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)





# Tarea de transformación de datos business
transform_data_business_task = PythonOperator(
    task_id="transform_data_business",
    python_callable=ETL_business,
    op_args=[ruta_bucket, "business.parquet"],    
    dag=dag,
)


# Tarea de transformación de datos tip
transform_data_tip_task = PythonOperator(
    task_id="transform_data_tip",
    python_callable=ETL_tip,
    op_args=[ruta_bucket, "tip.parquet"],
    dag=dag,
)



# Tarea de transformación de datos review
transform_data_review_task = PythonOperator(
    task_id="transform_data_review",
    python_callable=ETL_review,
    op_args=[ruta_bucket, "review.parquet"],
    dag=dag,
)


# Tarea de transformación de datos user
transform_data_user_task = PythonOperator(
    task_id="transform_data_user",
    python_callable=ETL_user,
    op_args=[ruta_bucket, "user.parquet"],
    dag=dag,
)



# Tarea de transformación de datos checkin
transform_data_checkin_task = PythonOperator(
    task_id="transform_data_checkin",
    python_callable=ETL_checkin,
    op_args=[ruta_bucket, "checkin.parquet"],
    dag=dag,
)




# Tareas de dependencia
transform_data_business_task >> transform_data_tip_task >> transform_data_review_task >> transform_data_user_task >> transform_data_checkin_task


