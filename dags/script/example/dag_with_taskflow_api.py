# from airflow.decorators import dag, task
# from datetime import datetime, timedelta 

# default_args = {
#     'owner':'animalstupid',
#     'retries':5,
#     'retry_delay': timedelta(minutes=5)
# }

# @dag( 
#     dag_id='dag_with_taskflow_api_v02',
#     default_args = default_args,
#     description='our first dag using python operator',
#     start_date=datetime(2024, 11, 9),
#     schedule_interval='@daily'
#     )

# def hello_world_etl():
#     @task(multiple_outputs=True)
#     def get_name():
#         return {
#             'first_name':'Nattapat',
#             'last_name':'Dungdee'
#             }
    
#     @task()
#     def get_age():
#         return 22

#     @task()
#     def greet(first_name, last_name, age):
#         print(f"Hello world!, My name is {first_name} {last_name} and i am {age} year old!")

#     name_dict = get_name()
#     age = get_age()
#     greet(first_name=name_dict['first_name'],
#           last_name=name_dict['last_name'],
#           age=age)


# #taskflow api take case push and pull operation 1:02:21
# greet_dag = hello_world_etl()