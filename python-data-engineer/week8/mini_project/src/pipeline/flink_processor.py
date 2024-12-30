"""
Flink stream processor implementation.
"""

import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    EnvironmentSettings,
    TableConfig,
    DataTypes,
    TableDescriptor,
    Schema
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FlinkProcessor:
    """Manages Flink stream processing."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Flink processor."""
        self.config = config
        self.env = None
        self.t_env = None
        self.job_name = config.get('job_name', 'data-sync-job')
        
        # Initialize Flink
        self._init_flink()
    
    def _init_flink(self):
        """Initialize Flink environments."""
        try:
            # Create stream environment
            self.env = StreamExecutionEnvironment.get_execution_environment()
            
            # Configure checkpointing
            checkpoint_interval = self.config.get('checkpoint_interval', 10000)
            self.env.enable_checkpointing(checkpoint_interval)
            
            # Configure parallelism
            parallelism = self.config.get('parallelism', 1)
            self.env.set_parallelism(parallelism)
            
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
            table_config.set_idle_state_retention(
                self.config.get('state_retention', 60 * 60 * 1000)  # 1 hour
            )
            
        except Exception as e:
            logger.error(f"Flink initialization failed: {e}")
            raise
    
    def create_source_table(
        self,
        table_name: str,
        topic: str,
        schema: Dict[str, Any]
    ) -> bool:
        """Create Kafka source table."""
        try:
            # Create table schema
            table_schema = Schema.new_builder()
            
            for field_name, field_type in schema.items():
                table_schema.column(
                    field_name,
                    self._get_flink_type(field_type)
                )
            
            # Add processing time attribute
            table_schema.column(
                'process_time',
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)
            ).column_by_expression(
                'process_time',
                'PROCTIME()'
            )
            
            # Create table
            self.t_env.create_temporary_table(
                table_name,
                TableDescriptor.for_connector('kafka')
                    .schema(table_schema.build())
                    .option('connector', 'kafka')
                    .option('topic', topic)
                    .option('properties.bootstrap.servers',
                           self.config['kafka']['bootstrap.servers'])
                    .option('format', 'json')
                    .option('json.fail-on-missing-field', 'false')
                    .option('json.ignore-parse-errors', 'true')
                    .build()
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Source table creation failed: {e}")
            return False
    
    def create_sink_table(
        self,
        table_name: str,
        topic: str,
        schema: Dict[str, Any]
    ) -> bool:
        """Create Kafka sink table."""
        try:
            # Create table schema
            table_schema = Schema.new_builder()
            
            for field_name, field_type in schema.items():
                table_schema.column(
                    field_name,
                    self._get_flink_type(field_type)
                )
            
            # Create table
            self.t_env.create_temporary_table(
                table_name,
                TableDescriptor.for_connector('kafka')
                    .schema(table_schema.build())
                    .option('connector', 'kafka')
                    .option('topic', topic)
                    .option('properties.bootstrap.servers',
                           self.config['kafka']['bootstrap.servers'])
                    .option('format', 'json')
                    .build()
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Sink table creation failed: {e}")
            return False
    
    def execute_query(self, query: str) -> bool:
        """Execute SQL query."""
        try:
            statement_set = self.t_env.create_statement_set()
            statement_set.add_insert_sql(query)
            
            job_client = statement_set.execute().get_job_client()
            if job_client:
                job_id = job_client.get_job_id()
                logger.info(f"Job submitted with ID: {job_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return False
    
    def _get_flink_type(self, type_str: str) -> DataTypes:
        """Convert string type to Flink DataType."""
        type_mapping = {
            'string': DataTypes.STRING(),
            'integer': DataTypes.INT(),
            'long': DataTypes.BIGINT(),
            'double': DataTypes.DOUBLE(),
            'boolean': DataTypes.BOOLEAN(),
            'timestamp': DataTypes.TIMESTAMP(3),
            'json': DataTypes.STRING()  # JSON stored as string
        }
        
        return type_mapping.get(type_str.lower(), DataTypes.STRING())
    
    def stop(self):
        """Stop Flink processing."""
        try:
            if self.env:
                self.env.stop()
        except Exception as e:
            logger.error(f"Flink stop failed: {e}") 