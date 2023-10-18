import sys
from etl_yelp import *

funcion = None if len(sys.argv)==1 else sys.argv[1]
print (f'Función a testear: {funcion if funcion else "Todas"}')

#bucket
#ruta_bucket = "gs://bucket-pghenry-dpt2"
ruta_bucket = "gs://bucket-steakhouses2"
# Test ETL_business
if not funcion or funcion=='ETL_business':
    print ('Testeando ETL_business...', end='')
    try:
        archivo = "business.parquet"
        ETL_business(ruta_bucket, 'business.parquet')
        print ('--> OK')
    except Exception as e:
        print('--> !ERROR')
        print (e)


# Test ETL_user
if not funcion or funcion=='ETL_user':
    print ('Testeando ETL_user...', end='')
    try:        
        archivo = "user.parquet"
        ETL_user(ruta_bucket, archivo)
        print ('--> OK')        
    except Exception as e:
        print('--> !ERROR')
        print (e)


# Test ETL_tip
if not funcion or funcion=='ETL_tip':
    print ('Testeando ETL_tip...', end='')
    try:        
        archivo = "tip.parquet"
        ETL_tip(ruta_bucket, archivo)
        print ('--> OK')        
    except Exception as e:
        print('--> !ERROR')
        print (e)



# Test ETL_review
if not funcion or funcion=='ETL_review':
    print ('Testeando ETL_review...', end='')
    try:        
        archivo = "review.parquet"
        ETL_review(ruta_bucket, archivo)
        print ('--> OK')        
    except Exception as e:
        print('--> !ERROR')
        print (e)


# Test ETL_checkin
if not funcion or funcion=='ETL_checkin':
    print ('Testeando ETL_checkin...', end='')
    try:        
        archivo = "checkin.parquet"
        ETL_checkin(ruta_bucket, archivo)
        print ('-> OK')        
    except Exception as e:
        print('--> !ERROR')
        print (e)
