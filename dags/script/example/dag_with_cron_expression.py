# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta 

# default_args = {
#     'owner':'animalstupid',
#     'retries':5,
#     'retry_delay': timedelta(minutes=5)
# }


# with DAG(
#     dag_id='dag_with_cron_expression_v06',
#     default_args = default_args,
#     start_date=datetime(2024, 11, 1),
#     schedule_interval='0 3 * * Tue-Fri', # minutes hour day(month) mount day(week)
#     catchup=True
# ) as dag:
#     task1 = BashOperator(
#         task_id='task1',
#         bash_command="echo dag with cron expression!"
#     )

#     task1