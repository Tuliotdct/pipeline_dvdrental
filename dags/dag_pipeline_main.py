from airflow.sdk import dag, task, TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

@dag(
    dag_id = 'dag_pipeline_main',
    schedule = '0 7 * * *',
    start_date = pendulum.datetime(2025,11,30, tz='Europe/Amsterdam'),
    catchup = False,
    tags = ['pipeline','medallion architecture', 'main']
)

def dag_pipeline_main():

    start = EmptyOperator(task_id = 'Start')

    trigger_bronze = TriggerDagRunOperator(
        task_id = 'bronze',
        trigger_dag_id = 'dag_pipeline_bronze',
        logical_date='{{ dag_run.logical_date }}',
        wait_for_completion = True,
        deferrable = True,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed']
    )

    trigger_silver = TriggerDagRunOperator(
        task_id = 'silver',
        trigger_dag_id = 'dag_pipeline_silver',
        logical_date='{{ dag_run.logical_date }}',
        wait_for_completion = True,
        deferrable = True,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed']
    )

    end = EmptyOperator(task_id = 'End')

    start >> trigger_bronze >> trigger_silver >> end

dag_pipeline_main()

