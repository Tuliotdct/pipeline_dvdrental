from unittest import result
import duckdb
import boto3
from dotenv import load_dotenv
import pendulum
import os
import logging
from .vars_airflow import get_variable


load_dotenv()

logger = logging.getLogger(__name__)


def silver_tables():

    # Get all the tables from the silver layer in S3

    s3 = boto3.client("s3")

    bucket = get_variable("BUCKET_NAME", default=os.getenv("BUCKET_NAME"))

    response = s3.list_objects_v2(Bucket = bucket, Prefix = 'silver/', Delimiter = '/')

    if 'CommonPrefixes' not in response:
        return []
    
    list_silver_tables = []
    for obj in response['CommonPrefixes']:
        filter_silver_tables = obj['Prefix'].split('/')[1]
        list_silver_tables.append(filter_silver_tables)

    
    return list_silver_tables


def gold_tables():
    """Returns the list of gold tables to be created"""
    return ['actor_in_films', 'customer_payment', 'rental_payment_customer']


def create_gold_table(table_name, partition_date = None):
    """Create a specific gold table"""
    
    bucket = get_variable("BUCKET_NAME", default=os.getenv("BUCKET_NAME"))
    region = get_variable("REGION_NAME", default=os.getenv("REGION_NAME"))

    if partition_date is None:
        # This will be triggered only for testing purposes
        partition_date = pendulum.datetime(2025, 11, 30, 0, 0, 0).format('YYYY-MM-DD_HH-mm-ss')

    # Get AWS credentials from boto3 session (works in MWAA)
    session = boto3.Session()
    credentials = session.get_credentials()
    
    # Check if we have temporary credentials (MWAA) or permanent credentials (local)
    if credentials and credentials.token:
        # Use temporary credentials from AWS environment (MWAA with IAM role)
        duckdb.sql(f"""CREATE OR REPLACE SECRET secret (
            TYPE s3,
            KEY_ID '{credentials.access_key}',
            SECRET '{credentials.secret_key}',
            SESSION_TOKEN '{credentials.token}',
            REGION '{region}'
        );
        """)
    else:
        # This reads from ~/.aws/credentials, environment variables, or .env
        duckdb.sql(f"""CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER config,
            REGION '{region}'
        );
        """)

    try:
        if table_name == 'actor_in_films':
            duckdb.sql(f"""
                       create or replace temp table actor_in_films as
                       select 
                        a.actor_id,
                        a.first_name as actor_first_name,
                        a.last_name as actor_last_name,
                        f.title as film_title,
                        f.release_year,
                        f.rating
                       from read_parquet('s3://{bucket}/silver/film/{partition_date}/film.parquet') as f
                       left join read_parquet('s3://{bucket}/silver/film_actor/{partition_date}/film_actor.parquet') as fa
                       on f.film_id = fa.film_id
                       left join read_parquet('s3://{bucket}/silver/actor/{partition_date}/actor.parquet') as a
                       on fa.actor_id = a.actor_id
                       where a.actor_id is not null     
                       """)
            
            duckdb.sql(f"""
                        COPY actor_in_films TO 's3://{bucket}/gold/actor_in_films/{partition_date}/actor_in_films.parquet';
                        """)
            logger.info(f'The parquet file actor_in_films.parquet successfully loaded into Gold S3')

        elif table_name == 'customer_payment':
            duckdb.sql(f"""
                create or replace temp table customer_payment as
                select 
                c.customer_id,
                c.first_name as customer_first_name,
                c.last_name as customer_last_name,
                c.email,
                p.amount as payment_amount,
                p.payment_date
                from read_parquet('s3://{bucket}/silver/customer/{partition_date}/customer.parquet') as c
                left join read_parquet('s3://{bucket}/silver/payment/{partition_date}/payment.parquet') as p
                on c.customer_id = p.customer_id
                """)
            
            duckdb.sql(f"""
                        COPY customer_payment TO 's3://{bucket}/gold/customer_payment/{partition_date}/customer_payment.parquet';
                        """)
            logger.info(f'The parquet file customer_payment.parquet successfully loaded into Gold S3')

        elif table_name == 'rental_payment_customer':
            duckdb.sql(f"""
                create or replace temp table rental_payment_customer as
                select 
                r.rental_date,
                return_date,
                p.amount as payment_amount,
                p.payment_date,
                c.first_name as customer_first_name,
                c.last_name as customer_last_name,
                from read_parquet('s3://{bucket}/silver/rental/{partition_date}/rental.parquet') as r
                left join read_parquet('s3://{bucket}/silver/payment/{partition_date}/payment.parquet') as p
                on r.rental_id = p.rental_id
                left join read_parquet('s3://{bucket}/silver/customer/{partition_date}/customer.parquet') as c
                on p.customer_id = c.customer_id
                """)
            
            duckdb.sql(f"""
                        COPY rental_payment_customer TO 's3://{bucket}/gold/rental_payment_customer/{partition_date}/rental_payment_customer.parquet';
                        """)
            logger.info(f'The parquet file rental_payment_customer.parquet successfully loaded into Gold S3')
        
        else:
            logger.error(f'Unknown table name: {table_name}')
            return False

        return True
    
    except Exception:
        logger.error(f'Failed to load parquet file {table_name} into S3')
        logger.error(f'Partition: {partition_date}, Bucket: {bucket}')
        raise