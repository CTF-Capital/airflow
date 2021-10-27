from airflow import DAG
from airflow.operators.python import task
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3 
import os
import pandas as pd
import logging
import time



def trades_QA(bucket):
    logging.info('Data QA: Starting.. \n-----------------')
    # Parameters 
    BUCKET = bucket # Bucket to analize (ENVIRONMENT VARIABLE)
    S3_BUCKET_PATH = f's3://{BUCKET}/'
    LOCAL_PATH = f'data_temp/'
    TODAY = str(datetime.today().date()).replace('-', '_')
    aws_access_key = os.environ.get('AWS_ACCESS_KEY')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    
    s3 = boto3.resource(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_access_key
                    )
    bucket = s3.Bucket(BUCKET)
    bucket_files = [x.key for x in bucket.objects.all()]
    bucket_files.sort()

    logging.info('\nProcess info', )
    logging.info(f'Date:{TODAY}')
    logging.info(f'Bucket: {BUCKET}')
    logging.info(f'Files in bucket: {len(bucket_files)}\n-----------------')



    badfiles = pd.DataFrame(dtype=str, columns=['file', 'exception'])
    logging.info('\n Quality assurance... \n')
    for file in bucket_files:
        s3.Bucket(BUCKET).download_file(file, LOCAL_PATH + file.split('/')[-1])
        time.sleep(1)
        # Try some transformations
        try:
            df = pd.read_csv(LOCAL_PATH + file.split('/')[-1], compression='gzip')
            df.rename(columns={'timestamp':'date_time', 'size':'volume'}, inplace=True)
            df = df[['date_time', 'price', 'volume']]
            df['price'] = df['price'].astype(float)
            df['volume'] = df['volume'].astype(float)
            df['date_time'] = pd.to_datetime(df['date_time'], unit='ms').astype('datetime64[m]')
            
            copy_source = {
                'Bucket': BUCKET,
                'Key': file
                }
            s3.meta.client.copy(copy_source, 'ctf-etl-trades', file)
            s3.Object(BUCKET, file).delete()
        except Exception as exp:
                badfiles = badfiles.append({'file':file, 'exception':str(exp)}, ignore_index=True)
                logging.info('Corrupted file:', file)
        
        # Delete data from local folder
        os.system(f'rm ' + LOCAL_PATH + file.split('/')[-1])

    logging.info('\nEnd of quality assurance \n-----------------')
    # Print log badfiles and save csv
    if (badfiles.shape[0] > 0):
        logging.info('\nCorrupted files: {badfiles.shape[0]}')
        badfiles.to_csv(LOCAL_PATH + f'badfiles_{BUCKET}_{TODAY}.csv', index=False)
        s3.Bucket('ctf-automations').upload_file(LOCAL_PATH + f'badfiles_{BUCKET}_{TODAY}.csv', 'Data_QA/' + f'badfiles_{BUCKET}_{TODAY}.csv')
    else:
        logging.info('\nNo corrupted data')
        
    logging.info('\nData QA: Completed')

default_args = {
    'email': ['federico.cardoso.e@gmail.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay':timedelta(minutes=5)
}
with DAG(
    'example_dag',
    start_date=datetime(2021, 8, 12),
    schedule_interval='0 0 * * *',
    default_args=default_args,
    catchup=False) as dag:

    data_QA = PythonOperator(
        task_id='data_QA',
        python_callable=trades_QA,
        op_kwargs={'bucket': 'ctf-etl-trades-test'}
    )


    data_QA

