from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from framework.dwh import Framework
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'test-framework',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

controller = Framework.get_controller("PRCS_TEST_FRAMEWORK2", 'prcs_nm')


with DAG(
    dag_id='PRCS_TEST_FRAMEWORK2',
    default_args=default_args,
    start_date = controller.calc_dt,
    schedule_interval=None,
) as dag:

    def create_trigger_tasks():
        # controller = Framework.get_controller("PRCS_TEST_FRAMEWORK2", 'prcs_nm')
        dependencies = controller.dpnd_prcs_nm
        trigger_tasks = []
        for dep_dag in dependencies:
            trigger_task = TriggerDagRunOperator(
                task_id=f'trigger_{dep_dag}',
                trigger_dag_id=dep_dag,
                conf={"message": "hi there3"},
                dag=dag,  # Associate with the DAG
            )
            trigger_tasks.append(trigger_task)
        return trigger_tasks

    trigger_tasks = create_trigger_tasks()

    # Set task dependencies
    trigger_tasks
