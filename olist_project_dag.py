
import logging
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import csv
from tempfile import NamedTemporaryFile
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



#Name of bucket where we will be loading our file daily
BUCKET_NAME = "s3a://kdb-etl-test/"

# Name of file according to day so that we can upload it to S3
todays_file_name = '{{ ds }}' + ".csv"

#making connection with AWS
s3_client = boto3.client('s3',region_name = 'ap-south-1',aws_access_key_id='AKIA3FN7P6DKMJMRK4GP', aws_secret_access_key='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')


#Defining DAG for incremental upload
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}
dag = DAG(
    default_args = default_args,
    start_date=datetime(2023,3,6),
    schedule_interval ='@daily',
    dag_id = "incremental_load",
    description = 'Incremental-load-lambda-project'
)


# STARTing of the DAG
start_data_pipeline = DummyOperator(task_id="extract_from_postgres", dag=dag)


#function for postgres operator for orders and order_itmes table(because they update every day)
def postgres_to_csv(table_name):
    step 1: query data from postgresql db and save into text file
    dt = '{{ ds }}'
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"\copy from (select * from {table_name} where dt = current_date) to '/airflow-local/dags/{table_name}/{dt}.csv' CSV HEADER;")
    with NamedTemporaryFile(mode='w', suffix=f"{dt}") as f:
    cursor.close()
    conn.close()


#function for postgres operator for remaining tables(they also update everyday but the data doesn't get
# as big as compared to orders and order items so we decided to load whole tables everyday)
def postgres_to_csv_2(table_name):
    step 1: query data from postgresql db and save into text file
    dt = '{{ ds }}'
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"\copy from (select * from {table_name}) to '/airflow-local/dags/{table_name}/{dt}.csv' CSV HEADER;")
    #with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}") as f:
    cursor.close()
    conn.close()



#### Extracting File from Postgres(or any OLTP database)
extract_orders = PythonOperator(
    dag=dag,
    task_id="extract_from_orders_table",
    python_callable=postgres_to_csv,
    op_kwargs = {"table_name":"orders"}
)

extract_order_items = PythonOperator(
    dag=dag,
    task_id="extract_from_order_items_table",
    python_callable=postgres_to_csv,
    op_kwargs = {"table_name":"order_items"}
)
extract_products = PythonOperator(
    dag=dag,
    task_id="extract_from_products_table",
    python_callable=postgres_to_csv_2,
    op_kwargs = {"table_name":"products"}
)
extract_customers = PythonOperator(
    dag=dag,
    task_id="extract_from_customers_table",
    python_callable=postgres_to_csv_2,
    op_kwargs = {"table_name":"customers"}
)
extract_categories = PythonOperator(
    dag=dag,
    task_id="extract_from_categories_table",
    python_callable=postgres_to_csv_2,
    op_kwargs = {"table_name":"categories"}
)
extract_departments = PythonOperator(
    dag=dag,
    task_id="extract_from_departments_table",
    python_callable=postgres_to_csv_2,
    op_kwargs = {"table_name":"departments"}
)



#upload function for S3 operator
def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3',region_name = 'ap-south-1',aws_access_key_id='AKIA3FN7P6DKMJMRK4GP', aws_secret_access_key='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')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

#s3 upload operators for each table files
insert_daily_file_to_s3_orders = PythonOperator(
    dag=dag,
    task_id="Incremental_insert_into_S3_orders",
    python_callable=upload_file,
    op_kwargs={"filename": f"dags/orders/{todays_file_name}", "bucket":f"orders"}
)

insert_daily_file_to_s3_order_items = PythonOperator(
    dag=dag,
    task_id="Incremental_insert_into_S3_order_items",
    python_callable=upload_file,
    op_kwargs={"filename": f"dags/order_items/{todays_file_name}", "bucket": f"order_items"}
)

insert_daily_file_to_s3_products = PythonOperator(
    dag=dag,
    task_id="Incremental_insert_into_S3_products",
    python_callable=upload_file,
    op_kwargs={"filename": f"dags/products/{todays_file_name}", "bucket": f"products"}
)


insert_daily_file_to_s3_customers = PythonOperator(
    dag=dag,
    task_id="Incremental_insert_into_S3_customers",
    python_callable=upload_file,
    op_kwargs={"filename": f"dags/customers/{todays_file_name}", "bucket":f"customers"}
)

insert_daily_file_to_s3_categories = PythonOperator(
    dag=dag,
    task_id="Incremental_insert_into_S3_categories",
    python_callable=upload_file,
    op_kwargs={"filename": f"dags/categories/{todays_file_name}", "bucket":f"categories"}
)

insert_daily_file_to_s3_departments = PythonOperator(
    dag=dag,
    task_id="Incremental_insert_into_S3_departments",
    python_callable=upload_file,
    op_kwargs={"filename": f"dags/departments/{todays_file_name}", "bucket":f"departments"}
)

loading_to_s3 = DummyOperator(task_id="loading_to_s3", dag=dag)
end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)
    
	
start_data_pipeline >> [extract_orders,extract_order_items,extract_products,extract_customers,extract_categories,extract_departments] >> loading_to_s3
loading_to_s3 >> [insert_daily_file_to_s3_orders,insert_daily_file_to_s3_order_items,insert_daily_file_to_s3_products,insert_daily_file_to_s3_customers,insert_daily_file_to_s3_categories,insert_daily_file_to_s3_departments] >> end_data_pipeline




