

# # Tarea de descarga de datos
# download_data_task = BashOperator(
#     task_id="download_data",
#     bash_command="curl https://example.com/data > data.csv",
#     dag=dag,
# )




# import google.auth

# credentials_info = {
#     "username": "flaviobovio2@gmail.com",
#     "password": "ping3328",
#     "project_id": "pghenry-dpt2 ",
# }

# #credentials = google.auth.credentials_from_service_account_info(credentials_info)
# #credentials = google.auth.default()
# print (credentials)


# # [START download_from_gdrive_to_local]
# FILE_NAME = 'https://drive.google.com/file/d/1oNBonW4G-CRt-jPTw93_2kvXLpEiJCIj'
# #FILE_NAME = 'https://drive.google.com/file/d/1r0wCAyB5h2d6heKRNtjRt8wlZTAJrYF5'
# OUTPUT_FILE = f'{ruta}/crudo/tip.parquet'
# # Configuración de las credenciales
# AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT = credentials

# download_from_gdrive_to_local = GoogleDriveToLocalOperator(
#     task_id="download_from_gdrive_to_local",
#     folder_id="",
#     file_name=FILE_NAME,
#     output_file=OUTPUT_FILE,
#     dag=dag,
# )
# # [END download_from_gdrive_to_local]
