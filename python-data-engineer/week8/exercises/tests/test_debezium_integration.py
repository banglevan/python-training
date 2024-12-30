"""
Debezium Integration Tests
---------------------

Test cases for:
1. Connector management
2. Event handling
3. Schema evolution
"""

import pytest
from unittest.mock import Mock, patch, call
import json
from datetime import datetime
import requests

from ..debezium_integration import DebeziumIntegration

@pytest.fixture
def mock_kafka_admin():
    """Create mock Kafka admin client."""
    with patch('confluent_kafka.admin.AdminClient') as mock:
        yield mock.return_value

@pytest.fixture
def mock_kafka_consumer():
    """Create mock Kafka consumer."""
    with patch('kafka.KafkaConsumer') as mock:
        consumer = Mock()
        mock.return_value = consumer
        yield consumer

@pytest.fixture
def mock_requests():
    """Create mock requests."""
    with patch('requests.post') as mock_post, \
         patch('requests.get') as mock_get, \
         patch('requests.delete') as mock_delete:
        yield {
            'post': mock_post,
            'get': mock_get,
            'delete': mock_delete
        }

@pytest.fixture
def debezium(mock_kafka_admin, mock_kafka_consumer):
    """Create Debezium integration instance."""
    integration = DebeziumIntegration(
        connect_url='http://localhost:8083',
        kafka_config={'bootstrap.servers': 'localhost:9092'},
        db_config={
            'dbname': 'test',
            'user': 'test',
            'password': 'test',
            'host': 'localhost'
        }
    )
    yield integration
    integration.close()

def test_create_connector(debezium, mock_requests):
    """Test connector creation."""
    # Setup mock response
    mock_requests['post'].return_value.status_code = 201
    
    # Test connector creation
    result = debezium.create_connector(
        'test-connector',
        {
            'database.hostname': 'localhost',
            'database.port': '5432',
            'database.user': 'test',
            'database.password': 'test',
            'database.dbname': 'test',
            'database.server.name': 'test'
        }
    )
    
    assert result == True
    mock_requests['post'].assert_called_once()
    
    # Test error handling
    mock_requests['post'].return_value.status_code = 400
    result = debezium.create_connector('test-connector', {})
    assert result == False

def test_delete_connector(debezium, mock_requests):
    """Test connector deletion."""
    mock_requests['delete'].return_value.status_code = 204
    
    result = debezium.delete_connector('test-connector')
    assert result == True
    mock_requests['delete'].assert_called_once()

def test_get_connector_status(debezium, mock_requests):
    """Test connector status check."""
    mock_requests['get'].return_value.status_code = 200
    mock_requests['get'].return_value.json.return_value = {
        'name': 'test-connector',
        'connector': {'state': 'RUNNING'}
    }
    
    status = debezium.get_connector_status('test-connector')
    assert status is not None
    assert status['connector']['state'] == 'RUNNING'

def test_handle_events(debezium, mock_kafka_consumer):
    """Test event handling."""
    # Setup mock messages
    mock_message = Mock()
    mock_message.value = {
        'schema': {},
        'payload': {'id': 1, 'name': 'test'}
    }
    
    mock_kafka_consumer.poll.return_value = {
        'topic': [mock_message]
    }
    
    # Test event handler
    handler = Mock()
    
    # Simulate event processing
    try:
        debezium.handle_events(
            ['test-topic'],
            handler,
            timeout=100
        )
    except StopIteration:
        pass
    
    mock_kafka_consumer.subscribe.assert_called_with(['test-topic'])
    handler.assert_called_with(mock_message.value)

@patch('psycopg2.connect')
def test_update_schema(mock_connect, debezium):
    """Test schema evolution."""
    # Setup mock cursor
    mock_cursor = Mock()
    mock_connect.return_value.__enter__.return_value.cursor.return_value = mock_cursor
    
    # Test schema update
    result = debezium.update_schema(
        'test_table',
        {
            'new_column': {'add': 'VARCHAR(100)'},
            'modified_column': {'modify': 'INTEGER'},
            'old_column': {'drop': True}
        }
    )
    
    assert result == True
    mock_cursor.execute.assert_called_once()
    
    # Verify ALTER TABLE statement
    call_args = mock_cursor.execute.call_args[0][0]
    assert 'ALTER TABLE test_table' in call_args
    assert 'ADD COLUMN new_column VARCHAR(100)' in call_args
    assert 'ALTER COLUMN modified_column TYPE INTEGER' in call_args
    assert 'DROP COLUMN old_column' in call_args 