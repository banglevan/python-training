"""
Flink CDC Tests
-----------

Test cases for:
1. Source configuration
2. Transformation logic
3. Sink integration
4. Pipeline execution
"""

import pytest
from unittest.mock import Mock, patch, call
import json
from datetime import datetime
from pyflink.table import (
    TableEnvironment,
    StreamTableEnvironment,
    EnvironmentSettings,
    TableConfig,
    ResultKind
)
from pyflink.datastream import StreamExecutionEnvironment

from ..flink_cdc import FlinkCDC

@pytest.fixture
def mock_stream_env():
    """Create mock Stream Execution Environment."""
    with patch('pyflink.datastream.StreamExecutionEnvironment') as mock:
        env = Mock()
        mock.get_execution_environment.return_value = env
        yield env

@pytest.fixture
def mock_table_env():
    """Create mock Table Environment."""
    with patch('pyflink.table.StreamTableEnvironment') as mock:
        t_env = Mock()
        mock.create.return_value = t_env
        yield t_env

@pytest.fixture
def flink_cdc(mock_stream_env, mock_table_env):
    """Create Flink CDC instance."""
    cdc = FlinkCDC(
        checkpoint_dir='file:///tmp/checkpoints',
        parallelism=1
    )
    yield cdc
    cdc.stop()

def test_initialization(flink_cdc, mock_stream_env):
    """Test environment initialization."""
    # Verify stream environment setup
    mock_stream_env.set_parallelism.assert_called_with(1)
    mock_stream_env.enable_checkpointing.assert_called_with(60000)
    
    # Verify checkpoint config
    mock_stream_env.get_checkpoint_config.return_value\
        .set_checkpoint_storage_dir.assert_called_with(
            'file:///tmp/checkpoints'
        )

def test_create_source_table(flink_cdc, mock_table_env):
    """Test source table creation."""
    # Test data
    connector_config = {
        'hostname': 'localhost',
        'port': '3306',
        'username': 'test',
        'password': 'test',
        'database-name': 'test',
        'table-name': 'users'
    }
    
    schema = {
        'id': 'BIGINT',
        'name': 'STRING',
        'email': 'STRING'
    }
    
    # Create source table
    flink_cdc.create_source_table(
        'source_users',
        connector_config,
        schema
    )
    
    # Verify SQL execution
    execute_sql_call = mock_table_env.execute_sql.call_args[0][0]
    
    # Check SQL components
    assert 'CREATE TABLE source_users' in execute_sql_call
    assert 'id BIGINT' in execute_sql_call
    assert 'name STRING' in execute_sql_call
    assert 'email STRING' in execute_sql_call
    assert "connector = 'mysql-cdc'" in execute_sql_call
    assert "hostname = 'localhost'" in execute_sql_call

def test_create_kafka_sink(flink_cdc, mock_table_env):
    """Test Kafka sink creation."""
    # Test data
    schema = {
        'id': 'BIGINT',
        'name': 'STRING',
        'email': 'STRING'
    }
    
    # Create Kafka sink
    flink_cdc.create_kafka_sink(
        'kafka_users',
        'users',
        'localhost:9092',
        schema
    )
    
    # Verify SQL execution
    execute_sql_call = mock_table_env.execute_sql.call_args[0][0]
    
    # Check SQL components
    assert 'CREATE TABLE kafka_users' in execute_sql_call
    assert "connector = 'kafka'" in execute_sql_call
    assert "topic = 'users'" in execute_sql_call
    assert "properties.bootstrap.servers = 'localhost:9092'" in execute_sql_call
    assert "format = 'json'" in execute_sql_call

def test_create_transformation(flink_cdc, mock_table_env):
    """Test transformation creation."""
    # Test transformation SQL
    transform_sql = """
    SELECT 
        id,
        name,
        email,
        CURRENT_TIMESTAMP as processed_at
    FROM source_users
    """
    
    # Create transformation
    flink_cdc.create_transformation(
        'enriched_users',
        'source_users',
        transform_sql
    )
    
    # Verify SQL execution
    execute_sql_call = mock_table_env.execute_sql.call_args[0][0]
    
    # Check SQL components
    assert 'CREATE VIEW enriched_users' in execute_sql_call
    assert 'SELECT' in execute_sql_call
    assert 'CURRENT_TIMESTAMP as processed_at' in execute_sql_call
    assert 'FROM source_users' in execute_sql_call

def test_create_pipeline(flink_cdc, mock_table_env):
    """Test pipeline creation and execution."""
    # Mock statement set
    statement_set = Mock()
    mock_table_env.create_statement_set.return_value = statement_set
    
    # Mock job client
    job_client = Mock()
    statement_set.execute.return_value.get_job_client.return_value = job_client
    job_client.get_job_id.return_value = 'test-job-1'
    
    # Create pipeline
    flink_cdc.create_pipeline(
        'source_users',
        'kafka_users',
        ['enriched_users']
    )
    
    # Verify statement creation
    statement_set.add_insert_sql.assert_called_once()
    insert_sql = statement_set.add_insert_sql.call_args[0][0]
    
    # Check SQL components
    assert 'INSERT INTO kafka_users' in insert_sql
    assert 'SELECT * FROM enriched_users' in insert_sql
    
    # Verify execution
    statement_set.execute.assert_called_once()
    job_client.get_job_execution_result.assert_called_once()

def test_error_handling(flink_cdc, mock_table_env):
    """Test error handling."""
    # Test source table creation error
    mock_table_env.execute_sql.side_effect = Exception("SQL Error")
    
    with pytest.raises(Exception):
        flink_cdc.create_source_table(
            'test_table',
            {},
            {'id': 'BIGINT'}
        )
    
    # Test pipeline execution error
    statement_set = Mock()
    mock_table_env.create_statement_set.return_value = statement_set
    statement_set.execute.side_effect = Exception("Execution Error")
    
    with pytest.raises(Exception):
        flink_cdc.create_pipeline('source', 'sink')

def test_cleanup(flink_cdc, mock_stream_env):
    """Test environment cleanup."""
    flink_cdc.stop()
    mock_stream_env.stop.assert_called_once() 