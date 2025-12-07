import pandas as pd
from .db_connections import get_connection
from sqlalchemy import inspect
import pendulum 
import boto3
import os
from dotenv import load_dotenv
import pyarrow

load_dotenv()

def create_bucket(bucket, region):

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

def get_db_tables(conn):
    
    # Inspect all the tables in the database
    insp = inspect(conn)
    db_tables = insp.get_table_names()

    return db_tables

def create_bronze_for_table(table):

    # Get one unique table from the database at once
    conn = get_connection()
    bucket = os.getenv('BUCKET_NAME')
    region = os.getenv('REGION_NAME')

    # time folder
    date_time = pendulum.now().format('YYYY-MM-DD_HH-mm-ss')

    # Call create_bucket function
    create_bucket(bucket, region)
    
    # Read table from database ---> Convert all table to parquet ---> Send parquet files to S3 
    try:
        df = pd.read_sql_table(con=conn, table_name=table)
        df.to_parquet(path=f's3://{bucket}/bronze/{table}/{date_time}/{table}.parquet', engine='pyarrow')

        print(f'{table}.parquet file successfully loaded into s3')

        return True

    except:
        print(f'{table}.parquet file was not successfully loaded into S3')

    return False


def create_bronze():

    # Get all the tables from the database at once.
    conn = get_connection()
    tables = get_db_tables(conn)
    for table in tables:
        create_bronze_for_table(table)

