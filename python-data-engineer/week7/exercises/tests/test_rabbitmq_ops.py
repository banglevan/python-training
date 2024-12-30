"""
RabbitMQ Operations Tests
----------------------

Test cases for:
1. Connection handling
2. Exchange operations
3. Queue operations
4. Message operations
5. DLQ functionality
"""

import pytest
import pika
import json
import time
from unittest.mock import Mock, patch
from ..rabbitmq_ops import RabbitMQOperator

@pytest.fixture
def rabbitmq_operator():
    """Create RabbitMQ operator instance."""
    return RabbitMQOperator(
        host='localhost',
        port=5672,
        username='guest',
        password='guest'
    )

@pytest.fixture
def mock_channel():
    """Create mock channel."""
    return Mock(spec=pika.channel.Channel)

@pytest.fixture
def mock_connection():
    """Create mock connection."""
    return Mock(spec=pika.connection.Connection)

def test_connection_context_manager(
    rabbitmq_operator,
    mock_channel,
    mock_connection
):
    """Test connection context manager."""
    with patch('pika.BlockingConnection') as mock_conn:
        mock_conn.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        
        with rabbitmq_operator.connection_channel() as channel:
            assert channel == mock_channel
        
        mock_connection.close.assert_called_once()

def test_declare_exchange(
    rabbitmq_operator,
    mock_channel,
    mock_connection
):
    """Test exchange declaration."""
    with patch('pika.BlockingConnection') as mock_conn:
        mock_conn.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        
        rabbitmq_operator.declare_exchange(
            'test_exchange',
            'direct',
            True
        )
        
        mock_channel.exchange_declare.assert_called_once_with(
            exchange='test_exchange',
            exchange_type='direct',
            durable=True
        )

def test_declare_queue(
    rabbitmq_operator,
    mock_channel,
    mock_connection
):
    """Test queue declaration."""
    with patch('pika.BlockingConnection') as mock_conn:
        mock_conn.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        
        rabbitmq_operator.declare_queue(
            'test_queue',
            True,
            'dlx',
            30000
        )
        
        mock_channel.queue_declare.assert_called_once_with(
            queue='test_queue',
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'dlx',
                'x-message-ttl': 30000
            }
        )

def test_bind_queue(
    rabbitmq_operator,
    mock_channel,
    mock_connection
):
    """Test queue binding."""
    with patch('pika.BlockingConnection') as mock_conn:
        mock_conn.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        
        rabbitmq_operator.bind_queue(
            'test_queue',
            'test_exchange',
            'test_key'
        )
        
        mock_channel.queue_bind.assert_called_once_with(
            queue='test_queue',
            exchange='test_exchange',
            routing_key='test_key'
        )

def test_publish_message(
    rabbitmq_operator,
    mock_channel,
    mock_connection
):
    """Test message publishing."""
    with patch('pika.BlockingConnection') as mock_conn:
        mock_conn.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        
        message = {'test': 'data'}
        rabbitmq_operator.publish_message(
            'test_exchange',
            'test_key',
            message,
            True
        )
        
        mock_channel.basic_publish.assert_called_once()
        call_args = mock_channel.basic_publish.call_args[1]
        assert call_args['exchange'] == 'test_exchange'
        assert call_args['routing_key'] == 'test_key'
        assert json.loads(
            call_args['body'].decode()
        ) == message

def test_setup_dead_letter_queue(
    rabbitmq_operator,
    mock_channel,
    mock_connection
):
    """Test DLQ setup."""
    with patch('pika.BlockingConnection') as mock_conn:
        mock_conn.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        
        rabbitmq_operator.setup_dead_letter_queue(
            'test_queue',
            'test_dlx',
            'test_dlq',
            30000
        )
        
        # Verify DLX declaration
        mock_channel.exchange_declare.assert_called_with(
            exchange='test_dlx',
            exchange_type='direct',
            durable=True
        )
        
        # Verify DLQ declaration
        mock_channel.queue_declare.assert_any_call(
            queue='test_dlq',
            durable=True,
            arguments={}
        )
        
        # Verify main queue declaration
        mock_channel.queue_declare.assert_any_call(
            queue='test_queue',
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'test_dlx',
                'x-message-ttl': 30000
            }
        )

def test_message_handler(
    mock_channel
):
    """Test message handler."""
    message = {'test': 'data'}
    method = Mock()
    method.delivery_tag = 1
    properties = Mock()
    body = json.dumps(message).encode()
    
    from ..rabbitmq_ops import message_handler
    message_handler(
        mock_channel,
        method,
        properties,
        body
    )
    
    mock_channel.basic_ack.assert_called_once_with(
        delivery_tag=1
    ) 