import boto3
import duckdb
import pendulum
import os
from dotenv import load_dotenv

load_dotenv()

def bronze_tables():
    
    # Get all the tables from the bronze layer in S3

    s3  = boto3.client("s3")

    bucket = os.getenv("BUCKET_NAME")

    response = s3.list_objects_v2(Bucket = bucket)

    if 'Contents' not in response:
        return []

    list_bronze_tables = []
    for obj in response['Contents']:
        filter_bronze_tables = obj['Key'].split("/")[1]
        list_bronze_tables.append(filter_bronze_tables)

    return list_bronze_tables

def create_silver_for_table(table, partition_date = None):

    # Transform data from the bronze layer and send to the Silver Layer

    bucket = os.getenv("BUCKET_NAME")
    region = os.getenv("REGION_NAME")

    if partition_date is None:
        partition_date = pendulum.now().format("YYYY-MM-DD_HH-mm-ss")

    duckdb.sql(f"""CREATE OR REPLACE SECRET secret (
        TYPE s3,
        PROVIDER config,
        REGION '{region}'
    );
    """
    )

    try:
        duckdb.sql(f"""
                   create or replace temp table tbl as
                   select distinct * from read_parquet('s3://{bucket}/bronze/{table}/*/*.parquet')""")
        
        duckdb.sql(f"""
                    COPY tbl TO 's3://{bucket}/silver/{table}/{partition_date}/{table}.parquet';
                    """)
        
        print(f'{table}.parquet file successfully loaded into s3')

        return True

    except:
        print(f'{table}.parquet file was not successfully loaded into S3')

    
    return False

def create_silver(partition_date=None):
    
    # Get all the tables from the bronze at once 
    
    tables = bronze_tables()
    for table in tables:
        create_silver_for_table(table=table, partition_date=None)



