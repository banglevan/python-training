"""
Airflow DAG for batch feature computation.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.data.sources.postgresql import PostgreSQLSource
from src.features.transformations.customer_transforms import CustomerFeatureTransformer
from src.features.transformations.product_transforms import ProductFeatureTransformer
from src.features.validation.validators import FeatureValidator
from src.data.sinks.feast import FeastSink
import pandas as pd
import logging

logger = logging.getLogger(__name__)

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

def extract_data(**context):
    """Extract data from PostgreSQL."""
    try:
        pg_source = PostgreSQLSource(
            connection_string="postgresql://user:pass@host:5432/db"
        )
        
        # Extract data for last 30 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        # Get customer orders
        customer_data = pg_source.extract_customer_features(
            start_date, end_date
        )
        
        # Get product data
        product_data = pg_source.extract_product_features(
            start_date, end_date
        )
        
        # Push to XCom for next tasks
        context['task_instance'].xcom_push(
            key='customer_data',
            value=customer_data.to_dict()
        )
        context['task_instance'].xcom_push(
            key='product_data',
            value=product_data.to_dict()
        )
        
    except Exception as e:
        logger.error(f"Data extraction failed: {e}")
        raise

def transform_features(**context):
    """Transform raw data into features."""
    try:
        # Get data from previous task
        ti = context['task_instance']
        customer_data = pd.DataFrame(
            ti.xcom_pull(key='customer_data')
        )
        product_data = pd.DataFrame(
            ti.xcom_pull(key='product_data')
        )
        
        # Initialize transformers
        customer_transformer = CustomerFeatureTransformer()
        product_transformer = ProductFeatureTransformer()
        
        # Transform customer features
        customer_features = pd.DataFrame([
            customer_transformer.compute_order_features(
                customer_data, customer_id
            )
            for customer_id in customer_data['customer_id'].unique()
        ])
        
        # Transform product features
        product_features = pd.DataFrame([
            product_transformer.compute_sales_features(
                product_data, product_id
            )
            for product_id in product_data['product_id'].unique()
        ])
        
        # Push transformed features to XCom
        context['task_instance'].xcom_push(
            key='customer_features',
            value=customer_features.to_dict()
        )
        context['task_instance'].xcom_push(
            key='product_features',
            value=product_features.to_dict()
        )
        
    except Exception as e:
        logger.error(f"Feature transformation failed: {e}")
        raise

def validate_features(**context):
    """Validate computed features."""
    try:
        # Get features from previous task
        ti = context['task_instance']
        customer_features = pd.DataFrame(
            ti.xcom_pull(key='customer_features')
        )
        product_features = pd.DataFrame(
            ti.xcom_pull(key='product_features')
        )
        
        # Initialize validator
        validator = FeatureValidator()
        
        # Validate features
        customer_validation = validator.validate_features(
            customer_features
        )
        product_validation = validator.validate_features(
            product_features
        )
        
        # Check validation results
        if not (customer_validation['passed'] and 
                product_validation['passed']):
            raise ValueError("Feature validation failed")
        
        # Push validated features to XCom
        context['task_instance'].xcom_push(
            key='validated_customer_features',
            value=customer_features.to_dict()
        )
        context['task_instance'].xcom_push(
            key='validated_product_features',
            value=product_features.to_dict()
        )
        
    except Exception as e:
        logger.error(f"Feature validation failed: {e}")
        raise

def store_features(**context):
    """Store features in Feast."""
    try:
        # Get validated features
        ti = context['task_instance']
        customer_features = pd.DataFrame(
            ti.xcom_pull(key='validated_customer_features')
        )
        product_features = pd.DataFrame(
            ti.xcom_pull(key='validated_product_features')
        )
        
        # Initialize Feast sink
        feast_sink = FeastSink(
            repo_path="feature_repo/",
            project="ml_feature_platform"
        )
        
        # Store features
        feast_sink.push_features_to_online_store(
            "customer_features",
            customer_features
        )
        feast_sink.push_features_to_online_store(
            "product_features",
            product_features
        )
        
        # Materialize features
        feast_sink.materialize_features(
            start_date=datetime.now() - timedelta(days=1),
            end_date=datetime.now()
        )
        
    except Exception as e:
        logger.error(f"Feature storage failed: {e}")
        raise

# Create DAG
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