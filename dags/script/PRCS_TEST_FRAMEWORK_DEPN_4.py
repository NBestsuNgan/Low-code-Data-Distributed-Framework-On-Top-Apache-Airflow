from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from framework.dwh import Framework
import sys, os, subprocess

controller = Framework.get_controller("PRCS_TEST_FRAMEWORK_DEPN_4", 'prcs_nm')


default_args = {
    'owner': 'test-framework',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

def get_param(**context):
    controller = context["dag_run"].conf['message']
    print(controller, datetime.now())


with DAG(
    dag_id = 'PRCS_TEST_FRAMEWORK_DEPN_4',
    default_args = default_args,
    start_date = controller.calc_dt,
    schedule_interval=None,  # Triggered externally

) as dag:
    recieve_param = PythonOperator(
        task_id='Run_bash_to_python_file',
        provide_context=True,
        python_callable=get_param,
    )

    recieve_param 
        