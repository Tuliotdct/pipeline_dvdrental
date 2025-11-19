import pandas as pd
from db_connections import get_connection
from sqlalchemy import inspect
import pendulum 
import boto3
import os
from dotenv import load_dotenv
import pyarrow


load_dotenv()
conn = get_connection()
bucket = os.getenv('BUCKET_NAME')
region = os.getenv('REGION_NAME')
date_time = pendulum.now().format('YYYY-MM-DD_HH-mm-ss')

def create_bucket():

    # Create s3 connection
    s3 = boto3.client('s3')

    # Create the bucket/folders on AWS S3
    try:
        response = s3.head_bucket(Bucket=bucket)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            s3_bucket = bucket
    except:
        s3_bucket = s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': region})
    
    return s3_bucket

def create_bronze():
    
    # Call create_bucket function
    create_bucket()
    
    # Inspect all the tables in the database
    insp = inspect(conn)
    db_tables = insp.get_table_names()

    # Read all tables from database ---> Convert all tables to parquet ---> Send parquet files to S3 
    for table in db_tables:
        try:
            db_tables = pd.read_sql_table(con=conn, table_name=table)
            parquet_files = db_tables.to_parquet(path=f's3://{bucket}/{table}/{date_time}/{table}.parquet', engine='pyarrow')
            
            print(f'{table}.parquet file successfully loaded into s3')

        except:
            print(f'{table}.parquet file was not successfully loaded into S3')

    return parquet_files


