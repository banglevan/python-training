"""
Kafka CDC Tests
-----------

Test cases for:
1. Connector management
2. Transform configurations
3. Monitoring
"""

import pytest
from unittest.mock import Mock, patch, call
import json
import requests
from confluent_kafka import KafkaError

from ..kafka_cdc import KafkaCDC

@pytest.fixture
def mock_admin():
    """Create mock Kafka admin client."""
    with patch('confluent_kafka.admin.AdminClient') as mock:
        yield mock.return_value

@pytest.fixture
def mock_producer():
    """Create mock Kafka producer."""
    with patch('confluent_kafka.Producer') as mock:
        producer = Mock()
        mock.return_value = producer
        yield producer

@pytest.fixture
def mock_requests():
    """Create mock requests."""
    with patch('requests.post') as mock_post, \
         patch('requests.get') as mock_get, \
         patch('requests.put') as mock_put, \
         patch('requests.delete') as mock_delete:
        yield {
            'post': mock_post,
            'get': mock_get,
            'put': mock_put,
            'delete': mock_delete
        }

@pytest.fixture
def cdc(mock_admin, mock_producer):
    """Create Kafka CDC instance."""
    cdc = KafkaCDC(
        bootstrap_servers='localhost:9092',
        connect_url='http://localhost:8083'
    )
    yield cdc
    cdc.close()

def test_setup_source_connector(cdc, mock_requests):
    """Test source connector setup."""
    # Setup mock response
    mock_requests['post'].return_value.status_code = 201
    
    # Test connector creation
    config = {
        'database.hostname': 'localhost',
        'database.port': '5432',
        'database.user': 'test',
        'database.password': 'test',
        'database.dbname': 'test',
        'database.server.name': 'test',
        'table.include.list': 'public.users'
    }
    
    result = cdc.setup_source_connector('test-source', config)
    assert result == True
    
    # Verify request
    mock_requests['post'].assert_called_once()
    call_args = mock_requests['post'].call_args
    assert call_args[1]['headers']['Content-Type'] == 'application/json'
    
    # Verify config
    sent_config = json.loads(call_args[1]['data'])
    assert sent_config['name'] == 'test-source'
    assert sent_config['connector.class'] == \
        'io.debezium.connector.postgresql.PostgresConnector'
    assert sent_config['database.hostname'] == 'localhost'

def test_setup_sink_connector(cdc, mock_requests):
    """Test sink connector setup."""
    mock_requests['post'].return_value.status_code = 201
    
    # Test sink creation
    config = {
        'connection.url': 'jdbc:postgresql://localhost:5432/test',
        'connection.user': 'test',
        'connection.password': 'test',
        'auto.create': 'true',
        'insert.mode': 'upsert'
    }
    
    result = cdc.setup_sink_connector(
        'test-sink',
        ['test.public.users'],
        config
    )
    assert result == True
    
    # Verify config
    call_args = mock_requests['post'].call_args
    sent_config = json.loads(call_args[1]['data'])
    assert sent_config['connector.class'] == \
        'io.confluent.connect.jdbc.JdbcSinkConnector'
    assert 'test.public.users' in sent_config['topics']

def test_setup_transforms(cdc, mock_requests):
    """Test transform setup."""
    # Mock get config
    mock_requests['get'].return_value.status_code = 200
    mock_requests['get'].return_value.json.return_value = {
        'connector.class': 'test.Connector',
        'tasks.max': '1'
    }
    
    # Mock update config
    mock_requests['put'].return_value.status_code = 200
    
    # Test transform setup
    transforms = [
        {
            'type': 'org.apache.kafka.connect.transforms.ExtractField$Key',
            'field': 'id'
        },
        {
            'type': 'org.apache.kafka.connect.transforms.ValueToKey',
            'fields': 'id'
        }
    ]
    
    result = cdc.setup_transforms('test-connector', transforms)
    assert result == True
    
    # Verify config update
    call_args = mock_requests['put'].call_args
    updated_config = json.loads(call_args[1]['data'])
    assert 'transforms.transform1.type' in updated_config
    assert 'transforms.transform2.type' in updated_config

def test_monitor_connector(cdc, mock_requests):
    """Test connector monitoring."""
    # Setup mock responses
    mock_requests['get'].return_value.status_code = 200
    mock_requests['get'].return_value.json.return_value = {
        'connector': {
            'state': 'RUNNING',
            'worker_id': 'test:8083'
        },
        'tasks': [
            {'state': 'RUNNING', 'id': 0}
        ]
    }
    
    # Test monitoring
    with patch('time.sleep') as mock_sleep:
        mock_sleep.side_effect = Exception("Stop monitoring")
        
        with pytest.raises(Exception):
            cdc.monitor_connector('test-connector', interval=1)
    
    mock_requests['get'].assert_called_with(
        'http://localhost:8083/connectors/test-connector/status'
    )

def test_delete_connector(cdc, mock_requests):
    """Test connector deletion."""
    mock_requests['delete'].return_value.status_code = 204
    
    result = cdc.delete_connector('test-connector')
    assert result == True
    
    mock_requests['delete'].assert_called_with(
        'http://localhost:8083/connectors/test-connector'
    )

def test_error_handling(cdc, mock_requests):
    """Test error handling."""
    # Test connector creation error
    mock_requests['post'].return_value.status_code = 500
    
    result = cdc.setup_source_connector('test', {})
    assert result == False
    
    # Test network error
    mock_requests['post'].side_effect = requests.exceptions.ConnectionError
    
    result = cdc.setup_source_connector('test', {})
    assert result == False
    
    # Test monitoring error
    mock_requests['get'].side_effect = requests.exceptions.RequestException
    
    with pytest.raises(Exception):
        cdc.monitor_connector('test')

def test_cleanup(cdc, mock_producer):
    """Test cleanup."""
    cdc.close()
    
    mock_producer.flush.assert_called_once()
    mock_producer.close.assert_called_once() 