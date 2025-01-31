"""
Weather Data ETL DAG
This DAG performs hourly ETL operations on weather data:
1. Extracts weather data from API for specified cities
2. Transforms the data with additional metrics
3. Loads the transformed data to a CSV file
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add project root to Python path
PROJECT_ROOT = '/media/sorour/8fe1c1c7-b574-4c1a-bf0c-dd93e66cb530/projects/DataEngineeringProjects/WeatherDataPipeline'
sys.path.append(PROJECT_ROOT)

from utils.database import Database
from utils.config import Config
from utils.etl import extract, transform, load

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# DAG configuration
DAG_ID = 'weather_data_etl'
SCHEDULE_INTERVAL = timedelta(hours=1)
DEFAULT_ARGS = {
    'owner': 'sorour',
    'start_date': datetime(2024, 1, 30),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Initialize configuration and database connection
def init_resources():
    """Initialize and return configuration and database connection."""
    config_path = os.path.join(PROJECT_ROOT, 'config.yaml')
    config = Config(config_path).config
    database = Database(config=config)
    database.test_connection()
    return config, database

config, database = init_resources()

# Task functions
def extract_task(**context):
    """
    Extract weather data from API and store in staging table.
    
    Args:
        context: Airflow context dictionary
    """
    try:
        cities = config['CITIES']
        extraction_timestamp = extract(cities=cities, config=config, database=database)
        logger.info(f"Extraction completed with timestamp: {extraction_timestamp}")
        context['ti'].xcom_push(key='extraction_timestamp', value=extraction_timestamp)
    except Exception as e:
        logger.error(f"Error in extract task: {str(e)}")
        raise

def transform_task(**context):
    """
    Transform staged weather data with additional metrics.
    
    Args:
        context: Airflow context dictionary
    """
    try:
        ti = context['ti']
        extraction_timestamp = ti.xcom_pull(task_ids='extract_task', key='extraction_timestamp')
        logger.info(f"Transform task received timestamp: {extraction_timestamp}")
        
        transformed_df = transform(extraction_timestamp=extraction_timestamp, database=database)
        logger.info(f"Transformed data shape: {transformed_df.shape}")
        
        ti.xcom_push(key='transformed_data', value=transformed_df.to_json())
    except Exception as e:
        logger.error(f"Error in transform task: {str(e)}")
        raise

def load_task(**context):
    """
    Load transformed data to CSV file.
    
    Args:
        context: Airflow context dictionary
    """
    try:
        ti = context['ti']
        transformed_df = ti.xcom_pull(task_ids='transform_task', key='transformed_data')
        output_path = config['PATHS']['ETL_OUTPUT_PATH']
        
        logger.info(f"Loading data to: {output_path}")
        df = pd.read_json(transformed_df)
        logger.info(f"Data to be loaded shape: {df.shape}")
        
        load(df=df, output_path=output_path)
    except Exception as e:
        logger.error(f"Error in load task: {str(e)}")
        raise

# DAG definition
dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule=SCHEDULE_INTERVAL,
    catchup=False,
    description='Hourly weather data ETL pipeline',
    tags=['etl', 'weather'],
    doc_md=__doc__
)

# Task definitions
extract_op = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task,
    dag=dag,
    doc_md=extract_task.__doc__
)

transform_op = PythonOperator(
    task_id='transform_task',
    python_callable=transform_task,
    dag=dag,
    doc_md=transform_task.__doc__
)

load_op = PythonOperator(
    task_id='load_task',
    python_callable=load_task,
    dag=dag,
    doc_md=load_task.__doc__
)

# Define task dependencies
extract_op >> transform_op >> load_op