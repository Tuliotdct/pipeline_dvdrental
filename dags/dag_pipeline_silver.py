from airflow.sdk import dag, task, TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
from src.silver import bronze_tables, create_silver_for_table
import pendulum

@dag(
    dag_id = 'dag_pipeline_silver',
    schedule = None,
    start_date = pendulum.datetime(2025,11,30),
    catchup = False,
    tags = ['pipeline','medallion architecture', 'silver']
)

def dag_pipeline_silver():
    
    start = EmptyOperator(task_id = 'Start')
    
    tables = bronze_tables()

    with TaskGroup(group_id = 'silver_jobs') as silver_group:
        for table in tables:
            @task(task_id=f'{table}')
            def load_single_table_silver(table_name = table, logical_date = None):
                partition_date = logical_date.in_timezone('Europe/Amsterdam').format('YYYY-MM-DD_HH-mm-ss')
                return create_silver_for_table(table=table_name, partition_date=partition_date)
            
            load_single_table_silver()

    end = EmptyOperator(task_id = 'End')


    start >> silver_group >> end

dag_pipeline_silver()