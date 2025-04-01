from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.movie_dataset_etl import MovieDatasetEtl
from airflow.exceptions import AirflowException
import os
import pandas as pd

# Moved inside functions to avoid serialization issues
# my_movie = MovieDatasetEtl("MOVIES", "MOVIE_BASE")  # Remove this global instance

def _save_to_parquet(extracted_data, parquet_base_path):
    """Save DataFrames to Parquet files with proper error handling"""
    try:
        os.makedirs(parquet_base_path, exist_ok=True)
        parquet_paths = {}
        
        for name, df in extracted_data.items():
            parquet_path = os.path.join(parquet_base_path, f"{name}.parquet")
            df.to_parquet(parquet_path, index=False)
            parquet_paths[name] = parquet_path
            
        return parquet_paths
    except Exception as e:
        raise AirflowException(f"Failed to save Parquet files: {str(e)}")

def _load_from_parquet(parquet_paths):
    """Load DataFrames from Parquet files with validation"""
    try:
        loaded_data = {}
        for name, path in parquet_paths.items():
            if not os.path.exists(path):
                raise FileNotFoundError(f"Parquet file {path} not found")
            loaded_data[name] = pd.read_parquet(path)
        return loaded_data
    except Exception as e:
        raise AirflowException(f"Failed to load Parquet files: {str(e)}")

def extract_and_transform_data(**kwargs):
    """Extract data from source"""
    ti = kwargs['ti']
    try:
        # Initialize inside function
        my_movie = MovieDatasetEtl("MOVIES", "MOVIE_BASE")
        
        extracted_data = my_movie.extract("./lib/assets/data/the-movies-dataset")
        transformed_data = my_movie.transform(extracted_data)
        
        # Save data to parquet
        parquet_base_path = "./lib/assets/data/parquet/transform"
        parquet_paths = _save_to_parquet(transformed_data, parquet_base_path)
        
        # Store paths dictionary in XCom
        ti.xcom_push(key='transform_paths', value=parquet_paths)
    
    except Exception as e:
        raise AirflowException(f"Extraction / Transformation failed: {str(e)}")

def load_data(**kwargs):
    """Load transformed data"""
    ti = kwargs['ti']
    try:
        # Get transformed paths
        transformed_paths = ti.xcom_pull(task_ids='extract_and_transform_data', key='transform_paths')
        
        # Load data
        transformed_data = _load_from_parquet(transformed_paths)
        
        # Initialize inside function
        my_movie = MovieDatasetEtl("MOVIES", "MOVIE_BASE")
        my_movie.load(transformed_data, "@S3_STAGE/etl/")

    except Exception as e:
        raise AirflowException(f"Loading failed: {str(e)}")  # Fixed error message

def update_data_mart(**kwargs):
    """Update data mart"""
    try:
        # Initialize inside function
        my_movie = MovieDatasetEtl("MOVIES", "MOVIE_BASE")
        my_movie.transform_to_mart()
        my_movie.snow_handler.close()
    except Exception as e:
        raise AirflowException(f"Data mart update failed: {str(e)}")
    
def cleanup_data(**kwargs):
    """Cleanup temporary files"""
    try:
        import shutil
        paths = [
            "./lib/assets/data/the-movies-dataset",
            "./lib/assets/data/parquet"
        ]
        for path in paths:
            if os.path.exists(path):
                shutil.rmtree(path)
    except Exception as e:
        raise AirflowException(f"Cleanup failed: {str(e)}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 8),
}

# Define the DAG
dag = DAG(
    'movie_data_pipeline',
    default_args=default_args,
    description='A movie data pipeline DAG with sequential tasks',
    schedule_interval='0 0 * * *', # timedelta(days=1)
    catchup=False,
    tags=['movie', 'etl'],
)

# Define the tasks
extract_and_transform_task = PythonOperator(
    task_id='extract_and_transform_data',
    python_callable=extract_and_transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

update_data_mart_task = PythonOperator(
    task_id='update_data_mart',
    python_callable=update_data_mart,
    provide_context=True,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_data',
    python_callable=cleanup_data,
    dag=dag,
)

# Set the task dependencies so that each task runs after the previous one
extract_and_transform_task >> load_task >> update_data_mart_task >> cleanup_task
