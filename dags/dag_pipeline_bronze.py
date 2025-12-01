from airflow.sdk import dag, task
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
from src.bronze import create_bronze_for_table, get_db_tables
from src.db_connections import get_connection
import pendulum


@dag(
    dag_id = 'dag_pipeline_bronze',
    schedule = '@daily',
    start_date = pendulum.datetime(2025,11,30),
    catchup  = False,
    tags = ['pipeline','medallion architecture', 'bronze']
)

def dag_pipeline_bronze():

    start = EmptyOperator(task_id = 'Start')
    conn = get_connection()
    tables = get_db_tables(conn)

    with TaskGroup(group_id = 'bronze_jobs') as bronze_group:
        
        for table in tables:
            @task(task_id = f'{table}')
            def load_single_table(table_name=table):
                return create_bronze_for_table(table_name)
            
            load_single_table()
            
    end = EmptyOperator(task_id = 'End')

    start >> bronze_group >> end

dag_pipeline_bronze()

