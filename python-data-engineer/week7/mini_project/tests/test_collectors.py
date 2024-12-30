"""
Collectors Tests
------------

Test cases for data collection components.
"""

import pytest
from unittest.mock import Mock, patch
import json
from datetime import datetime

from pipeline.collectors import MQTTCollector
from pipeline.models import SensorData

@pytest.fixture
def mock_mqtt_client():
    """Create mock MQTT client."""
    with patch('paho.mqtt.client.Client') as mock:
        yield mock.return_value

@pytest.fixture
def callback():
    """Create mock callback."""
    return Mock()

@pytest.fixture
def collector(mock_mqtt_client, callback):
    """Create MQTT collector instance."""
    collector = MQTTCollector(
        broker='localhost',
        port=1883,
        topics=['sensors/+/data'],
        callback=callback
    )
    yield collector
    collector.stop()

def test_mqtt_start(collector, mock_mqtt_client):
    """Test MQTT collector start."""
    collector.start()
    
    mock_mqtt_client.connect.assert_called_once_with(
        'localhost',
        1883,
        60
    )
    mock_mqtt_client.loop_start.assert_called_once()

def test_mqtt_stop(collector, mock_mqtt_client):
    """Test MQTT collector stop."""
    collector.stop()
    
    mock_mqtt_client.loop_stop.assert_called_once()
    mock_mqtt_client.disconnect.assert_called_once()

def test_mqtt_connection(collector, mock_mqtt_client):
    """Test MQTT connection handling."""
    # Simulate successful connection
    collector._on_connect(None, None, None, 0)
    
    mock_mqtt_client.subscribe.assert_called()
    
    # Simulate failed connection
    with pytest.raises(Exception):
        collector._on_connect(None, None, None, 1)

def test_mqtt_message_handling(collector, callback):
    """Test MQTT message handling."""
    # Create test message
    message = Mock()
    message.topic = 'sensors/test_sensor/data'
    message.payload = json.dumps({
        'metric': 'temperature',
        'value': 25.5,
        'timestamp': datetime.now().isoformat()
    }).encode()
    
    # Process message
    collector._on_message(None, None, message)
    
    # Verify callback
    callback.assert_called_once()
    data = callback.call_args[0][0]
    assert isinstance(data, SensorData)
    assert data.sensor_id == 'test_sensor'
    assert data.value == 25.5

def test_error_handling(collector, callback):
    """Test error handling."""
    # Test invalid message
    message = Mock()
    message.topic = 'sensors/test/data'
    message.payload = b'invalid json'
    
    collector._on_message(None, None, message)
    callback.assert_not_called() 