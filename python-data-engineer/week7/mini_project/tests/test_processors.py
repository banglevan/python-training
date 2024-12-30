"""
Processors Tests
------------

Test cases for data processing components.
"""

import pytest
from unittest.mock import Mock, patch
import json
from datetime import datetime

from pipeline.processors import DataProcessor

@pytest.fixture
def mock_kafka_producer():
    """Create mock Kafka producer."""
    with patch('kafka.KafkaProducer') as mock:
        yield mock.return_value

@pytest.fixture
def mock_kafka_consumer():
    """Create mock Kafka consumer."""
    with patch('kafka.KafkaConsumer') as mock:
        yield mock.return_value

@pytest.fixture
def processor(mock_kafka_producer, mock_kafka_consumer):
    """Create processor instance."""
    processor = DataProcessor(
        kafka_servers='localhost:9092',
        input_topic='input',
        output_topic='output',
        group_id='test_group'
    )
    yield processor
    processor.stop()

def test_processor_start(processor, mock_kafka_consumer):
    """Test processor start."""
    # Setup mock messages
    message = Mock()
    message.value = {
        'sensor_id': 'test',
        'metric': 'temperature',
        'value': '25.5',
        'timestamp': datetime.now().isoformat()
    }
    mock_kafka_consumer.__iter__.return_value = [message]
    
    # Start processor
    processor.start()
    
    # Verify message processing
    mock_kafka_consumer.__iter__.assert_called_once()

def test_message_processing(processor, mock_kafka_producer):
    """Test message processing."""
    # Test data
    data = {
        'sensor_id': 'test',
        'metric': 'temperature',
        'value': '25.5',
        'timestamp': datetime.now().isoformat()
    }
    
    # Process message
    result = processor._process_message(data)
    
    # Verify processing
    assert result['sensor_id'] == data['sensor_id']
    assert result['metric'] == data['metric']
    assert isinstance(result['value'], float)
    assert 'processed_at' in result

def test_invalid_message(processor):
    """Test invalid message handling."""
    # Test missing fields
    invalid_data = {
        'sensor_id': 'test'
    }
    
    with pytest.raises(ValueError):
        processor._process_message(invalid_data)

def test_processor_stop(processor):
    """Test processor stop."""
    processor.stop()
    assert not processor.running 