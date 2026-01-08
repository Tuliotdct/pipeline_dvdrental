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
