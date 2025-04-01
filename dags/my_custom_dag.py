from airflow import DAG
from datetime import datetime
from operators.my_custom_operator import MyCustomOperator

# Define the DAG
dag = DAG(
    dag_id="my_custom_dag",
    start_date=datetime(2024, 4, 1),
    schedule_interval="@daily",
    catchup=False
)

# Use the custom operator
custom_task = MyCustomOperator(
    task_id="custom_task",
    my_param="Hello, Airflow!",
    dag=dag
)

custom_task