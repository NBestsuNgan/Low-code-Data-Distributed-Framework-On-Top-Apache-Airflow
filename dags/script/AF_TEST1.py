from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
from framework.dwh import Framework
import sys, os, subprocess

strem_controller = Framework.get_controller("AF_TEST1", 'strem_nm')

default_args = {
    'owner': f"{strem_controller.owner}-stream",
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

def print_message_func():
    print(list_of_test_data)

with DAG(
    dag_id = 'AF_TEST1',
    default_args = default_args,
    start_date = strem_controller.calc_dt,
    schedule_interval = strem_controller.cron_express,
) as dag:    
    print_message = PythonOperator(
        task_id='Run_bash_to_python_file',
        provide_context=True,
        python_callable=print_message_func,
    )

    list_of_test_data = []

    def create_trigger_tasks():
        #get prcs_nm by order but trigger all at once
        trigger_tasks = []
        for prcs_grp in range(len(strem_controller.prcs_grp)):
            if strem_controller.prcs_grp_act_f[prcs_grp] != 0:
                prcs_grp_controller = Framework.get_controller(f"{strem_controller.prcs_grp[prcs_grp]}", 'prcs_grp')
                for prcs_nm in range(len(prcs_grp_controller.prcs_nm)):
                    if prcs_grp_controller.prcs_act_f[prcs_nm] != 0:
                        trigger_task = TriggerDagRunOperator(
                            task_id=f'trigger_{prcs_grp_controller.prcs_nm[prcs_nm]}_prcsgrp_{prcs_grp_controller.prcs_grp[prcs_nm]}_prcsgrpprir_{strem_controller.prcs_grp_prir[prcs_grp]}_prcsprir_{prcs_grp_controller.prcs_prir[prcs_nm]}',
                            trigger_dag_id=prcs_grp_controller.prcs_nm[prcs_nm],
                            dag=dag,  # Associate with the DAG AF_TEST1
                        )
                        trigger_tasks.append([trigger_task])
                        list_of_test_data.append([trigger_task])
        return trigger_tasks

    trigger_tasks = create_trigger_tasks()


    # Set dependencies dynamically
    for i in range(len(trigger_tasks) - 1): # 0 , before 1
        for task in trigger_tasks[i]: #0
            for next_task in trigger_tasks[i + 1]:
                task >> next_task

    last_phase_tasks = trigger_tasks[len(trigger_tasks) - 1]
    for task in last_phase_tasks:
        task >> print_message
