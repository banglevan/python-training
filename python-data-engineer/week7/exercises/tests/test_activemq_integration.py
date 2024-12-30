"""
ActiveMQ Integration Tests
----------------------

Test cases for:
1. Connection handling
2. Queue operations
3. Topic operations
4. Message handling
5. Subscription management
"""

import pytest
import stomp
import json
from unittest.mock import Mock, patch
from ..activemq_integration import (
    ActiveMQClient,
    QueueListener,
    TopicListener
)

@pytest.fixture
def activemq_client():
    """Create ActiveMQ client instance."""
    return ActiveMQClient(
        host='localhost',
        port=61613,
        username='admin',
        password='admin'
    )

@pytest.fixture
def mock_connection():
    """Create mock STOMP connection."""
    return Mock(spec=stomp.Connection)

def test_connect(
    activemq_client,
    mock_connection
):
    """Test connection establishment."""
    with patch('stomp.Connection') as mock_conn:
        mock_conn.return_value = mock_connection
        activemq_client.conn = mock_connection
        
        activemq_client.connect()
        
        mock_connection.connect.assert_called_once_with(
            'admin',
            'admin',
            wait=True
        )

def test_disconnect(
    activemq_client,
    mock_connection
):
    """Test connection termination."""
    with patch('stomp.Connection') as mock_conn:
        mock_conn.return_value = mock_connection
        activemq_client.conn = mock_connection
        mock_connection.is_connected.return_value = True
        
        activemq_client.disconnect()
        
        mock_connection.disconnect.assert_called_once()

def test_send_to_queue(
    activemq_client,
    mock_connection
):
    """Test queue message sending."""
    with patch('stomp.Connection') as mock_conn:
        mock_conn.return_value = mock_connection
        activemq_client.conn = mock_connection
        
        message = {'test': 'data'}
        activemq_client.send_to_queue(
            'test.queue',
            message,
            True
        )
        
        mock_connection.send.assert_called_once()
        call_args = mock_connection.send.call_args[1]
        assert call_args['destination'] == '/queue/test.queue'
        assert json.loads(call_args['body']) == message
        assert call_args['headers']['persistent'] == 'true'

def test_send_to_topic(
    activemq_client,
    mock_connection
):
    """Test topic message publishing."""
    with patch('stomp.Connection') as mock_conn:
        mock_conn.return_value = mock_connection
        activemq_client.conn = mock_connection
        
        message = {'test': 'data'}
        activemq_client.send_to_topic(
            'test.topic',
            message
        )
        
        mock_connection.send.assert_called_once()
        call_args = mock_connection.send.call_args[1]
        assert call_args['destination'] == '/topic/test.topic'
        assert json.loads(call_args['body']) == message

def test_subscribe_to_queue(
    activemq_client,
    mock_connection
):
    """Test queue subscription."""
    with patch('stomp.Connection') as mock_conn:
        mock_conn.return_value = mock_connection
        activemq_client.conn = mock_connection
        
        callback = Mock()
        activemq_client.subscribe_to_queue(
            'test.queue',
            callback
        )
        
        mock_connection.set_listener.assert_called_once()
        mock_connection.subscribe.assert_called_once()
        call_args = mock_connection.subscribe.call_args[1]
        assert call_args['destination'] == '/queue/test.queue'
        assert call_args['ack'] == 'client-individual'

def test_subscribe_to_topic(
    activemq_client,
    mock_connection
):
    """Test topic subscription."""
    with patch('stomp.Connection') as mock_conn:
        mock_conn.return_value = mock_connection
        activemq_client.conn = mock_connection
        
        callback = Mock()
        activemq_client.subscribe_to_topic(
            'test.topic',
            callback,
            'consumer1',
            True
        )
        
        mock_connection.set_client_id.assert_called_once_with(
            'consumer1'
        )
        mock_connection.set_listener.assert_called_once()
        mock_connection.subscribe.assert_called_once()
        call_args = mock_connection.subscribe.call_args[1]
        assert call_args['destination'] == '/topic/test.topic'
        assert call_args['ack'] == 'client-individual'
        assert call_args['headers']['activemq.subscriptionName'] == 'consumer1'

def test_unsubscribe(
    activemq_client,
    mock_connection
):
    """Test unsubscription."""
    with patch('stomp.Connection') as mock_conn:
        mock_conn.return_value = mock_connection
        activemq_client.conn = mock_connection
        
        # Add subscription
        activemq_client.subscriptions['test.topic'] = 'sub1'
        
        activemq_client.unsubscribe('test.topic')
        
        mock_connection.unsubscribe.assert_called_once_with(
            'sub1'
        )
        assert 'test.topic' not in activemq_client.subscriptions

def test_queue_listener():
    """Test queue message listener."""
    callback = Mock()
    listener = QueueListener(callback)
    
    frame = Mock()
    frame.body = json.dumps({'test': 'data'})
    frame.headers = {'header1': 'value1'}
    
    listener.on_message(frame)
    
    callback.assert_called_once_with(
        {'test': 'data'},
        {'header1': 'value1'}
    )

def test_topic_listener():
    """Test topic message listener."""
    callback = Mock()
    listener = TopicListener(callback)
    
    frame = Mock()
    frame.body = json.dumps({'test': 'data'})
    frame.headers = {'header1': 'value1'}
    
    listener.on_message(frame)
    
    callback.assert_called_once_with(
        {'test': 'data'},
        {'header1': 'value1'}
    ) 