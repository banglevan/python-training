"""
Airflow DAG for feature computation.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.pipelines.batch.feature_computation_dag import (
    extract_data,
    transform_features,
    validate_features,
    store_features
)

default_args = {
    'owner': 'ml_platform',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'feature_computation',
    default_args=default_args,
    description='Compute and store features',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_features',
    python_callable=transform_features,
    provide_context=True,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_features',
    python_callable=validate_features,
    provide_context=True,
    dag=dag
)

store_task = PythonOperator(
    task_id='store_features',
    python_callable=store_features,
    provide_context=True,
    dag=dag
)

# Set task dependencies
extract_task >> transform_task >> validate_task >> store_task 