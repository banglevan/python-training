"""
Kafka Operations Tests
------------------

Test cases for:
1. Topic management
2. Message production
3. Consumer operations
4. Stream processing
"""

import pytest
from unittest.mock import Mock, patch
from kafka.admin import NewTopic, ConfigResource
from kafka.errors import KafkaError
import json
from datetime import datetime

from ..kafka_ops import KafkaOperator, example_processor

@pytest.fixture
def kafka_operator():
    """Create Kafka operator instance."""
    with patch('kafka.KafkaAdminClient'), \
         patch('kafka.KafkaProducer'):
        operator = KafkaOperator(
            bootstrap_servers='localhost:9092',
            client_id='test_client'
        )
        yield operator
        operator.close()

@pytest.fixture
def mock_admin():
    """Create mock admin client."""
    with patch('kafka.KafkaAdminClient') as mock:
        yield mock.return_value

@pytest.fixture
def mock_producer():
    """Create mock producer."""
    with patch('kafka.KafkaProducer') as mock:
        yield mock.return_value

@pytest.fixture
def mock_consumer():
    """Create mock consumer."""
    with patch('kafka.KafkaConsumer') as mock:
        yield mock.return_value

def test_create_topic(
    kafka_operator,
    mock_admin
):
    """Test topic creation."""
    # Test data
    topic = 'test_topic'
    partitions = 3
    replication = 1
    config = {
        'retention.ms': '86400000',
        'cleanup.policy': 'delete'
    }
    
    # Create topic
    kafka_operator.create_topic(
        topic,
        partitions,
        replication,
        config
    )
    
    # Verify admin client call
    mock_admin.create_topics.assert_called_once()
    call_args = mock_admin.create_topics.call_args[0][0]
    new_topic = call_args[0]
    
    assert isinstance(new_topic, NewTopic)
    assert new_topic.name == topic
    assert new_topic.num_partitions == partitions
    assert new_topic.replication_factor == replication
    assert new_topic.topic_configs == config

def test_delete_topic(
    kafka_operator,
    mock_admin
):
    """Test topic deletion."""
    topic = 'test_topic'
    
    kafka_operator.delete_topic(topic)
    
    mock_admin.delete_topics.assert_called_once_with(
        [topic]
    )

def test_get_topic_config(
    kafka_operator,
    mock_admin
):
    """Test topic configuration retrieval."""
    topic = 'test_topic'
    
    # Mock config entries
    mock_entry = Mock()
    mock_entry.name = 'retention.ms'
    mock_entry.value = '86400000'
    
    mock_config = Mock()
    mock_config.entries = [mock_entry]
    
    mock_admin.describe_configs.return_value = [
        mock_config
    ]
    
    config = kafka_operator.get_topic_config(topic)
    
    assert config['retention.ms'] == '86400000'
    mock_admin.describe_configs.assert_called_once()

def test_produce_message(
    kafka_operator,
    mock_producer
):
    """Test message production."""
    # Test data
    topic = 'test_topic'
    message = {
        'id': 1,
        'value': 'test'
    }
    key = 'test_key'
    
    # Mock future result
    mock_future = Mock()
    mock_metadata = Mock()
    mock_metadata.partition = 0
    mock_metadata.offset = 1
    mock_future.get.return_value = mock_metadata
    
    mock_producer.send.return_value = mock_future
    
    # Produce message
    kafka_operator.produce_message(
        topic,
        message,
        key
    )
    
    # Verify producer call
    mock_producer.send.assert_called_once()
    call_args = mock_producer.send.call_args[1]
    
    assert call_args['topic'] == topic
    assert json.loads(
        call_args['value'].decode()
    ) == message
    assert call_args['key'] == key

def test_create_consumer(
    kafka_operator,
    mock_consumer
):
    """Test consumer creation."""
    group_id = 'test_group'
    
    consumer = kafka_operator.create_consumer(
        group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    assert consumer == mock_consumer

def test_stream_processor():
    """Test stream processor function."""
    message = {
        'id': 1,
        'value': 5
    }
    
    result = example_processor(message)
    
    assert 'processed_at' in result
    assert result['value'] == 10

@pytest.mark.asyncio
async def test_parallel_process_stream(
    kafka_operator
):
    """Test parallel stream processing."""
    input_topic = 'input_topic'
    output_topic = 'output_topic'
    group_id = 'test_group'
    
    with patch.object(
        kafka_operator,
        'process_stream'
    ) as mock_process:
        # Start parallel processing
        await kafka_operator.parallel_process_stream(
            input_topic,
            output_topic,
            example_processor,
            group_id,
            num_workers=3
        )
        
        # Verify process_stream was called for each worker
        assert mock_process.call_count == 3

def test_error_handling(
    kafka_operator,
    mock_admin,
    mock_producer
):
    """Test error handling."""
    # Test topic creation error
    mock_admin.create_topics.side_effect = KafkaError(
        "Topic creation failed"
    )
    
    with pytest.raises(Exception):
        kafka_operator.create_topic('test_topic')
    
    # Test message production error
    mock_producer.send.side_effect = KafkaError(
        "Message production failed"
    )
    
    with pytest.raises(Exception):
        kafka_operator.produce_message(
            'test_topic',
            {'test': 'data'}
        ) 