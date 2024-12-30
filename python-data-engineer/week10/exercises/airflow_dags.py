"""
Airflow DAGs exercise.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import logging

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['alert@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def _process_user(ti):
    """Process user data from API."""
    users = ti.xcom_pull(task_ids=['extract_user'])
    if not users:
        raise ValueError('No user data available')
    
    user = users[0]
    processed_user = {
        'user_id': user['id'],
        'username': user['username'],
        'email': user['email'].lower(),
        'processed_at': datetime.now().isoformat()
    }
    
    ti.xcom_push(key='processed_user', value=processed_user)

def _store_user():
    """Store user data in PostgreSQL."""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    stored = False
    
    try:
        hook.run("""
            INSERT INTO users (user_id, username, email, processed_at)
            VALUES (%(user_id)s, %(username)s, %(email)s, %(processed_at)s)
        """, parameters=processed_user)
        stored = True
    except Exception as e:
        logging.error(f"Error storing user: {e}")
        raise
    
    return stored

# Create DAG
with DAG(
    'user_processing',
    default_args=default_args,
    description='User processing pipeline',
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # Create user table
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100) NOT NULL,
                processed_at TIMESTAMP NOT NULL
            );
        """
    )
    
    # Check if API is available
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='users/'
    )
    
    # Extract user from API
    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='users/1',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
    
    # Process user
    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user
    )
    
    # Store user
    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user
    )
    
    # Generate report
    generate_report = BashOperator(
        task_id='generate_report',
        bash_command='echo "Report generated for user {{ ti.xcom_pull(task_ids=["process_user"])[0] }}" > /tmp/report.txt'
    )
    
    # Set dependencies
    create_table >> is_api_available >> extract_user >> process_user >> store_user >> generate_report 