from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from src.bronze import create_bronze
import pendulum


@dag(
    dag_id = 'dag_pipeline_bronze',
    schedule = None,
    start_date = pendulum.datetime(2025,11,30)
)

def dag_pipeline_bronze():

    start = EmptyOperator(task_id = 'Start')

    @task(task_id = 'extract')
    def extract():
        bronze = create_bronze()
        return bronze

    extract_task = extract()

    end = EmptyOperator(task_id = 'End')

    start >> extract_task >> end

dag_pipeline_bronze()