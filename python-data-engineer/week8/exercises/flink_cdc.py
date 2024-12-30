"""
Flink CDC Implementation
------------------

Practice with:
1. Source connectors
2. Processing operators
3. Sink integration
"""

import logging
from typing import Dict, Any, List, Optional
import json
import time
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    EnvironmentSettings,
    TableConfig
)
from pyflink.table.descriptors import (
    Json,
    Kafka,
    Schema,
    Rowtime
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FlinkCDC:
    """Flink CDC implementation."""
    
    def __init__(
        self,
        checkpoint_dir: str,
        parallelism: int = 1
    ):
        """Initialize Flink CDC."""
        # Create execution environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(parallelism)
        self.env.enable_checkpointing(60000)  # 60 seconds
        self.env.get_checkpoint_config().set_checkpoint_storage_dir(
            checkpoint_dir
        )
        
        # Create table environment
        self.t_env = StreamTableEnvironment.create(
            self.env,
            environment_settings=EnvironmentSettings
                .new_instance()
                .in_streaming_mode()
                .build()
        )
        
        # Configure state TTL
        table_config = self.t_env.get_config()
        table_config.set_idle_state_retention_time(
            datetime.timedelta(hours=1),
            datetime.timedelta(hours=2)
        )
    
    def create_source_table(
        self,
        name: str,
        connector_config: Dict[str, Any],
        schema: Dict[str, str]
    ):
        """Create source table."""
        try:
            # Build DDL statement
            columns = [
                f"{col_name} {col_type}"
                for col_name, col_type in schema.items()
            ]
            
            ddl = f"""
                CREATE TABLE {name} (
                    {', '.join(columns)},
                    PRIMARY KEY (id) NOT ENFORCED
                ) WITH (
                    'connector' = 'mysql-cdc',
                    {', '.join(
                        f"'{k}' = '{v}'"
                        for k, v in connector_config.items()
                    )}
                )
            """
            
            self.t_env.execute_sql(ddl)
            logger.info(f"Created source table: {name}")
            
        except Exception as e:
            logger.error(f"Source table creation failed: {e}")
            raise
    
    def create_kafka_sink(
        self,
        name: str,
        topic: str,
        bootstrap_servers: str,
        schema: Dict[str, str]
    ):
        """Create Kafka sink table."""
        try:
            # Build DDL statement
            columns = [
                f"{col_name} {col_type}"
                for col_name, col_type in schema.items()
            ]
            
            ddl = f"""
                CREATE TABLE {name} (
                    {', '.join(columns)}
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = '{topic}',
                    'properties.bootstrap.servers' = '{bootstrap_servers}',
                    'format' = 'json'
                )
            """
            
            self.t_env.execute_sql(ddl)
            logger.info(f"Created Kafka sink: {name}")
            
        except Exception as e:
            logger.error(f"Kafka sink creation failed: {e}")
            raise
    
    def create_transformation(
        self,
        name: str,
        source: str,
        transformation_sql: str
    ):
        """Create transformation view."""
        try:
            # Create view with transformation
            view_sql = f"""
                CREATE VIEW {name} AS
                {transformation_sql}
            """
            
            self.t_env.execute_sql(view_sql)
            logger.info(f"Created transformation: {name}")
            
        except Exception as e:
            logger.error(f"Transformation creation failed: {e}")
            raise
    
    def create_pipeline(
        self,
        source: str,
        sink: str,
        transformations: Optional[List[str]] = None
    ):
        """Create and execute pipeline."""
        try:
            # Build pipeline query
            if transformations:
                # Apply transformations
                query = f"""
                    INSERT INTO {sink}
                    SELECT * FROM {transformations[-1]}
                """
            else:
                # Direct source to sink
                query = f"""
                    INSERT INTO {sink}
                    SELECT * FROM {source}
                """
            
            # Execute pipeline
            statement_set = self.t_env.create_statement_set()
            statement_set.add_insert_sql(query)
            
            job_client = statement_set.execute().get_job_client()
            if job_client:
                job_id = job_client.get_job_id()
                logger.info(f"Pipeline started with job ID: {job_id}")
                
                # Wait for job completion
                job_client.get_job_execution_result().result()
                
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
    
    def stop(self):
        """Stop Flink execution."""
        try:
            self.env.stop()
        except Exception as e:
            logger.error(f"Environment cleanup failed: {e}")

def main():
    """Run Flink CDC example."""
    cdc = FlinkCDC(
        checkpoint_dir='file:///tmp/flink-checkpoints',
        parallelism=1
    )
    
    try:
        # Create source table
        source_config = {
            'hostname': 'localhost',
            'port': '3306',
            'username': 'root',
            'password': 'root',
            'database-name': 'sourcedb',
            'table-name': 'users'
        }
        
        source_schema = {
            'id': 'BIGINT',
            'name': 'STRING',
            'email': 'STRING',
            'created_at': 'TIMESTAMP(3)'
        }
        
        cdc.create_source_table(
            'source_users',
            source_config,
            source_schema
        )
        
        # Create transformations
        cdc.create_transformation(
            'enriched_users',
            'source_users',
            """
            SELECT
                id,
                name,
                email,
                created_at,
                CURRENT_TIMESTAMP as processed_at
            FROM source_users
            """
        )
        
        # Create Kafka sink
        cdc.create_kafka_sink(
            'kafka_users',
            'users',
            'localhost:9092',
            {
                'id': 'BIGINT',
                'name': 'STRING',
                'email': 'STRING',
                'created_at': 'TIMESTAMP(3)',
                'processed_at': 'TIMESTAMP(3)'
            }
        )
        
        # Execute pipeline
        cdc.create_pipeline(
            'source_users',
            'kafka_users',
            ['enriched_users']
        )
        
    finally:
        cdc.stop()

if __name__ == '__main__':
    main() 