from .db_connections import get_connection
import pandas as pd
import boto3
from dotenv import load_dotenv
import os
from .vars_airflow import get_variable


load_dotenv()

def count_rows_in_db_dvdrental(table_name):
    """Count rows in a specific table from the database."""
    conn = get_connection()
    count_table = pd.read_sql(f"select '{table_name}' as table_name, count(*) as qtd_rows from {table_name}", con=conn)
    return count_table


def count_rows_in_s3_bronze_dvdrental(table_name):
    """Count rows in a specific table from S3 bronze layer (latest partition)."""
    bucket = get_variable("BUCKET_NAME", default=os.getenv("BUCKET_NAME"))
    s3 = boto3.client("s3")
    
    # Find latest partition
    partitions_paginator = s3.get_paginator('list_objects_v2')
    partitions_iterator = partitions_paginator.paginate(Bucket=bucket, Prefix=f'bronze/{table_name}/', Delimiter='/')

    latest_partition = None
    for partition_page in partitions_iterator:
        if 'CommonPrefixes' not in partition_page:
            continue
        
        for partition in partition_page['CommonPrefixes']:
            partition_prefix = partition['Prefix']

            if latest_partition is None:
                latest_partition = partition_prefix
            elif partition_prefix > latest_partition:
                latest_partition = partition_prefix

    if latest_partition is None:
        raise ValueError(f"No partition found for table {table_name} in bronze layer")

    s3_path = f's3://{bucket}/{latest_partition}{table_name}.parquet'

    try:
        df = pd.read_parquet(s3_path)
        return pd.DataFrame([{
            'table_name': table_name,
            'qtd_rows': len(df)
        }])
    except Exception as e:
        raise Exception(f"Error reading {s3_path}: {e}")


def dq_quality_checks(table_name):
    """Perform data quality checks between DB and S3 bronze layer for a specific table."""
    
    count_db = count_rows_in_db_dvdrental(table_name)
    count_s3_bronze = count_rows_in_s3_bronze_dvdrental(table_name)

    dq_check = count_db.merge(count_s3_bronze, on='table_name', how='outer')
    dq_check.rename(columns={'qtd_rows_x': 'qtd_rows_db', 'qtd_rows_y': 'qtd_rows_s3_bronze'}, inplace=True)

    dq_check['qtds_dq_check'] = dq_check['qtd_rows_db'] == dq_check['qtd_rows_s3_bronze']
    
    # If check fails, raise an error
    if not dq_check['qtds_dq_check'].all():
        row = dq_check.iloc[0]
        raise ValueError(
            f"Data quality check failed for {table_name}: "
            f"DB rows={row['qtd_rows_db']}, S3 rows={row['qtd_rows_s3_bronze']}"
        )

    return dq_check
