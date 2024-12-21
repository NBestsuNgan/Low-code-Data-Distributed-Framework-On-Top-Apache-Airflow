# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta 

# default_args = {
#     'owner':'animalstupid',
#     'retries':5,
#     'retry_delay': timedelta(minutes=5)
# }

# with DAG(
#     dag_id='dag_with_catchup_backfill_v02',
#     default_args = default_args,
#     start_date=datetime(2024, 11, 1),
#     schedule_interval='@daily',
#     catchup=True
# ) as dag:
#     task1 = BashOperator(
#         task_id='task1',
#         bash_command="echo this is a simple bash command"
#     )

#     task1