from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define some example Python functions for tasks
def extract_data():
    print("Extracting data...")
    # Simulate data extraction logic here
    return "data_extracted"

def transform_data(extracted_data):
    print(f"Transforming data: {extracted_data}")
    # Simulate data transformation logic here
    return "data_transformed"

def load_data(transformed_data):
    print(f"Loading data: {transformed_data}")
    # Simulate data loading logic here
    return "data_loaded"

def send_notification(status):
    print(f"Sending notification: Data processing {status}")
    # Simulate sending a notification or email
    return f"Notification sent for {status}"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 8),
}

# Define the DAG
dag = DAG(
    'complex_data_pipeline',
    default_args=default_args,
    description='A complex data pipeline DAG with sequential tasks',
    schedule_interval=timedelta(days=1), # '0 0 * * 1-5' -> run every weekend at midnight
    catchup=False,  # Do not run past executions
)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={'extracted_data': "{{ task_instance.xcom_pull(task_ids='extract_data') }}"},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={'transformed_data': "{{ task_instance.xcom_pull(task_ids='transform_data') }}"},
    dag=dag,
)

notify_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
    op_kwargs={'status': "{{ task_instance.xcom_pull(task_ids='load_data') }}"},
    dag=dag,
)

# Set the task dependencies so that each task runs after the previous one
extract_task >> transform_task >> load_task >> notify_task
