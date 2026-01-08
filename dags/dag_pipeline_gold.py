from airflow.sdk import dag, task, TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
from src.gold import gold_tables, create_gold_table
import pendulum


@dag(
    dag_id = 'dag_pipeline_gold',
    schedule = None,
    start_date = pendulum.datetime(2025,11,30),
    catchup = False,
    tags = ['pipeline','medallion architecture', 'gold'] 
)

def dag_pipeline_gold():

    start = EmptyOperator(task_id = 'Start')
    
    with TaskGroup(group_id = 'gold_jobs') as gold_group:
        @task
        def load_single_table_gold(table_name, logical_date = None):
            partition_date = logical_date.in_timezone('Europe/Amsterdam').format('YYYY-MM-DD_HH-mm-ss')
            return create_gold_table(table_name, partition_date=partition_date)
        
        # Criar uma task para cada tabela gold
        tables = gold_tables()
        for table in tables:
            load_single_table_gold.override(task_id = f'{table}')(table_name = table)

    end = EmptyOperator(task_id = 'End')


    start >> gold_group >> end


dag_pipeline_gold()