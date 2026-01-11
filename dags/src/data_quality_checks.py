from .db_connections import get_connection
import pandas as pd
import boto3
from sqlalchemy import inspect
from dotenv import load_dotenv
import os
from .vars_airflow import get_variable


load_dotenv()

def count_rows_in_db_dvdrental():
    # Row count of all tables in dvdrental database

    conn = get_connection()
    ins = inspect(conn)
    tables = ins.get_table_names()

    count_db_dvdrental = []
    for table in tables:
        count_table = pd.read_sql(f"select '{table}' as table_name, count(*) as qtd_rows from {table}", con=conn)
        count_db_dvdrental.append(count_table)

    df_count_db_dvdrental = pd.concat(count_db_dvdrental, ignore_index=True)

    return df_count_db_dvdrental


def count_rows_in_s3_bronze_dvdrental():

    bucket = get_variable("BUCKET_NAME", default=os.getenv("BUCKET_NAME"))

    s3 = boto3.client("s3")

    # Use paginator to handle large number of objects
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix='bronze/', Delimiter='/')

    count_s3_dvdrental = []
    for page in page_iterator:
        if 'CommonPrefixes' not in page:
            continue
        
        for obj in page['CommonPrefixes']:
            table_name = obj['Prefix'].split('/')[1]

            # Use paginator for partitions as well
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

            s3_path = f's3://{bucket}/{latest_partition}{table_name}.parquet'

            try:
                df = pd.read_parquet(s3_path)
                count_s3_dvdrental.append({
                    'table_name': table_name,
                    'qtd_rows': len(df)
                })

            
            except Exception as e:
                print(f"Error reading {s3_path}: {e}")
                continue

        df_count_s3_dvdrental = pd.DataFrame(count_s3_dvdrental)

    return df_count_s3_dvdrental


def dq_quality_checks():
    count_db = count_rows_in_db_dvdrental()
    count_s3_bronze = count_rows_in_s3_bronze_dvdrental()



    dq_check = count_db.merge(count_s3_bronze, on='table_name', how='outer')
    dq_check.rename(columns={'qtd_rows_x': 'qtd_rows_db', 'qtd_rows_y': 'qtd_rows_s3_bronze'}, inplace=True)

    dq_check['qtds_dq_check'] = dq_check['qtd_rows_db'] == dq_check['qtd_rows_s3_bronze']

    return dq_check
