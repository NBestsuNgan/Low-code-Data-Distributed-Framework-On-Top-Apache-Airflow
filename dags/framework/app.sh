#!/bin/bash

# Resolve the directory of the current script (app.sh)
script_dir=$(dirname "$0")

# Path to your Python script relative to the location of app.sh
python_script="$script_dir/../script/app.py"

container_id=$(docker ps --filter "name=airflow_docker-airflow-webserver-1" --format "{{.ID}}")
# Execute the Python script with the passed parameters
# python3 "$python_script" "$@"
docker exec -it $container_id python3 /opt/airflow/dags/script/app.py "$@"
# echo "Viewing Airflow webserver logs:"
# echo "tail -f $AIRFLOW_HOME/logs/airflow-webserver.log"
# tail -f $AIRFLOW_HOME/logs/airflow-webserver.log
# docker exec -it 0e9af0fd3538 python3 "$python_script" "$@"
