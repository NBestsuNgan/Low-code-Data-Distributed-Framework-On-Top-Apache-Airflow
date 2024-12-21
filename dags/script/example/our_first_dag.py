# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta 

# default_args = {
#     'owner':'animalstupid',
#     'retries':5,
#     'retry_delay': timedelta(minutes=2)
# }

# with DAG(
#     dag_id='our-first-dag_v5',
#     default_args = default_args,
#     description='this is our first dag',
#     start_date=datetime(2024, 11, 9, 2),
#     schedule_interval='@daily'
# ) as dag:
#     task1 = BashOperator(
#         task_id='first_task',
#         bash_command="echo hello world, this is first task!"
#     )

#     task2 = BashOperator(
#         task_id='second_task',
#         bash_command="echo hey, im task 2 and i will be run after task 1"
#     )

#     task3 = BashOperator(
#         task_id='thrid_task',
#         bash_command="echo hey, im task 3 and i will be run after task 1 at the same time as task 2"
#     )

#     #task dependency method 1
#     # task1 >> task2
#     # task1 >> task3

#     #task dependency method 2
#     # task1.set_downstream(task2)
#     # task1.set_downstream(task3)

#     #task dependency method 3
#     task1 >> [task2, task3]



