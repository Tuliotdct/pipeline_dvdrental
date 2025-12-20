from airflow.sdk import dag, task, TaskGroup, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from src.bronze import create_bronze_for_table, get_db_tables, create_bucket
from src.db_connections import get_connection
import pendulum
import os


@dag(
    dag_id = 'dag_pipeline_bronze',
    schedule = None,
    start_date = pendulum.datetime(2025,11,30),
    catchup  = False,
    tags = ['pipeline','medallion architecture', 'bronze']
)

def dag_pipeline_bronze():

    start = EmptyOperator(task_id = 'Start')

    bucket = Variable.get('BUCKET_NAME', default=os.getenv('BUCKET_NAME'))
    region = Variable.get('REGION_NAME', default=os.getenv('REGION_NAME'))

    @task(task_id = 'create_bucket')
    def create_bucket_once():
        create_bucket(bucket, region)
    
    task_create_bucket = create_bucket_once()

    conn = get_connection()
    tables = get_db_tables(conn)

    with TaskGroup(group_id = 'bronze_jobs') as bronze_group:
        @task
        def load_single_table_bronze(table_name, logical_date=None):
            partition_date = logical_date.in_timezone('Europe/Amsterdam').format('YYYY-MM-DD_HH-mm-ss')
            return create_bronze_for_table(table_name, partition_date=partition_date)
        
        for table in tables:
            load_single_table_bronze.override(task_id=f'{table}')(table_name=table)
        
            
    end = EmptyOperator(task_id = 'End')

    start >> task_create_bucket >> bronze_group >> end

dag_pipeline_bronze()

