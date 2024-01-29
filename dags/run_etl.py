#!/usr/bin/env python

import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta

# Resolve the home directory path
home_dir = os.path.expanduser("~")

default_args = {
    'owner': 'Hicham',
    'start_date': datetime(2023, 1, 26),
    'retries': 3,
    'retry_delay': timedelta(minutes=6),
}

# Function to determine if create_tables task should be skipped
def should_skip_create_tables(**kwargs):
    # Check if a file indicating that the create_tables task has already been run exists
    file_path = f"{home_dir}/Project/Movies-Recommendations-FR/script/create_tables_completed.txt"
    if os.path.exists(file_path):
        return "run_transform_load_data"
    else:
        return "run_create_tables"

# Create an instance of the DAG
dag = DAG(
    'Run_ETL',
    default_args=default_args,
    description='Data pipeline for collecting and analyzing data from TheMovieDB',
    schedule_interval=timedelta(minutes=60),
    catchup=False,
)

# Run Task to get data
extract_data = BashOperator(
    task_id='run_get_data_task',
    bash_command=f"python3 {home_dir}/Project/Movies-Recommendations-FR/script/extract_data.py",
    dag=dag,
)

# Check if create_tables task should be skipped
skip_create_tables = ShortCircuitOperator(
    task_id='skip_create_tables',
    python_callable=should_skip_create_tables,
    provide_context=True,
    dag=dag,
)

# Create tables of datawarehouse and data marts (only if not skipped)
create_tables = BashOperator(
    task_id='run_create_tables',
    bash_command=f"python3 {home_dir}/Project/Movies-Recommendations-FR/script/create_tables.py;touch {home_dir}/Project/Movies-Recommendations-FR/script/create_tables_completed.txt;",
    dag=dag,
)

# Transform and load data into DW and Datamarts
transform_load = BashOperator(
    task_id='run_transform_load_data',
    bash_command=f"python3 {home_dir}/Project/Movies-Recommendations-FR/script/transform_load.py",
    dag=dag,
)

# Run tasks
extract_data >> skip_create_tables >> create_tables >> transform_load

if __name__ == "__main__":
    dag.cli()