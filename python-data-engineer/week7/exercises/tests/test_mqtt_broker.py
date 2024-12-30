"""
MQTT Broker Tests
-------------

Test cases for:
1. Connection management
2. Message publishing
3. Subscriptions
4. QoS handling
5. Retained messages
"""

import pytest
from unittest.mock import Mock, patch
import paho.mqtt.client as mqtt
import json
from datetime import datetime

from ..mqtt_broker import MQTTBroker, message_handler

@pytest.fixture
def mqtt_broker():
    """Create MQTT broker instance."""
    with patch('paho.mqtt.client.Client') as mock_client:
        broker = MQTTBroker(
            client_id='test_client',
            host='localhost',
            port=1883
        )
        yield broker
        broker.disconnect()

@pytest.fixture
def mock_client():
    """Create mock MQTT client."""
    with patch('paho.mqtt.client.Client') as mock:
        yield mock.return_value

def test_connect(
    mqtt_broker,
    mock_client
):
    """Test broker connection."""
    mqtt_broker.connect()
    
    mock_client.connect.assert_called_once_with(
        'localhost',
        1883,
        60
    )
    mock_client.loop_start.assert_called_once()

def test_disconnect(
    mqtt_broker,
    mock_client
):
    """Test broker disconnection."""
    mqtt_broker.disconnect()
    
    mock_client.loop_stop.assert_called_once()
    mock_client.disconnect.assert_called_once()

def test_publish(
    mqtt_broker,
    mock_client
):
    """Test message publishing."""
    topic = 'test/topic'
    message = {'test': 'data'}
    qos = 1
    retain = True
    
    # Mock publish result
    mock_result = Mock()
    mock_result.mid = 123
    mock_client.publish.return_value = mock_result
    
    mid = mqtt_broker.publish(
        topic,
        message,
        qos,
        retain
    )
    
    assert mid == 123
    mock_client.publish.assert_called_once()
    call_args = mock_client.publish.call_args[0]
    assert call_args[0] == topic
    assert json.loads(call_args[1]) == message
    assert call_args[2] == qos
    assert call_args[3] == retain

def test_subscribe(
    mqtt_broker,
    mock_client
):
    """Test topic subscription."""
    topic = 'test/topic'
    callback = Mock()
    qos = 2
    
    # Mock subscribe result
    mock_client.subscribe.return_value = (mqtt.MQTT_ERR_SUCCESS, 1)
    
    mqtt_broker.subscribe(topic, callback, qos)
    
    mock_client.subscribe.assert_called_once_with(
        topic,
        qos=qos
    )
    assert mqtt_broker.topic_callbacks[topic] == callback

def test_unsubscribe(
    mqtt_broker,
    mock_client
):
    """Test topic unsubscription."""
    topic = 'test/topic'
    
    # Add callback first
    mqtt_broker.topic_callbacks[topic] = Mock()
    
    # Mock unsubscribe result
    mock_client.unsubscribe.return_value = (mqtt.MQTT_ERR_SUCCESS, 1)
    
    mqtt_broker.unsubscribe(topic)
    
    mock_client.unsubscribe.assert_called_once_with(topic)
    assert topic not in mqtt_broker.topic_callbacks

def test_on_message(
    mqtt_broker,
    mock_client
):
    """Test message handling."""
    topic = 'test/topic'
    message = {'test': 'data'}
    callback = Mock()
    
    # Register callback
    mqtt_broker.topic_callbacks[topic] = callback
    
    # Create mock message
    mock_message = Mock()
    mock_message.topic = topic
    mock_message.payload = json.dumps(message).encode()
    mock_message.qos = 1
    mock_message.retain = False
    
    # Trigger message handler
    mqtt_broker._on_message(None, None, mock_message)
    
    callback.assert_called_once_with(
        topic,
        message,
        1,
        False
    )

def test_retained_messages(
    mqtt_broker,
    mock_client
):
    """Test retained message handling."""
    topic = 'test/topic'
    message = {'test': 'data'}
    
    # Create mock retained message
    mock_message = Mock()
    mock_message.topic = topic
    mock_message.payload = json.dumps(message).encode()
    mock_message.retain = True
    
    # Trigger message handler
    mqtt_broker._on_message(None, None, mock_message)
    
    assert mqtt_broker.retained_messages[topic] == message

def test_wait_for_publish(
    mqtt_broker,
    mock_client
):
    """Test publish confirmation waiting."""
    mid = 123
    mqtt_broker.message_ids[mid] = False
    
    # Simulate publish confirmation
    mqtt_broker._on_publish(None, None, mid)
    
    result = mqtt_broker.wait_for_publish(mid, timeout=1)
    assert result == True

def test_tls_config(mock_client):
    """Test TLS configuration."""
    broker = MQTTBroker(
        client_id='test_client',
        use_tls=True
    )
    
    mock_client.tls_set.assert_called_once()

def test_error_handling(
    mqtt_broker,
    mock_client
):
    """Test error handling."""
    # Test connection error
    mock_client.connect.side_effect = Exception(
        "Connection failed"
    )
    
    with pytest.raises(Exception):
        mqtt_broker.connect()
    
    # Test publish error
    mock_client.publish.side_effect = Exception(
        "Publish failed"
    )
    
    with pytest.raises(Exception):
        mqtt_broker.publish(
            'test/topic',
            {'test': 'data'}
        ) 