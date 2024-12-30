"""
IoT Integration Tests
----------------

Test cases for:
1. Device management
2. Message routing
3. Security verification
4. Connection handling
"""

import pytest
from unittest.mock import Mock, patch
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import uuid
import base64
import hmac
import hashlib

from ..iot_integration import (
    IoTIntegration,
    Device
)

@pytest.fixture
def iot_integration():
    """Create IoT integration instance."""
    with patch('paho.mqtt.client.Client') as mock_client:
        integration = IoTIntegration(
            broker_host='localhost',
            broker_port=8883,
            ca_certs='test.crt'
        )
        yield integration
        integration.stop()

@pytest.fixture
def mock_client():
    """Create mock MQTT client."""
    with patch('paho.mqtt.client.Client') as mock:
        yield mock.return_value

@pytest.fixture
def test_device():
    """Create test device instance."""
    return Device(
        device_id='test_device',
        type='sensor',
        location='test_room',
        capabilities=['temperature'],
        auth_key='test_key',
        metadata={'test': 'data'}
    )

def test_start(
    iot_integration,
    mock_client
):
    """Test integration start."""
    iot_integration.start()
    
    mock_client.connect.assert_called_once_with(
        'localhost',
        8883,
        keepalive=60
    )
    mock_client.loop_start.assert_called_once()

def test_stop(
    iot_integration,
    mock_client
):
    """Test integration stop."""
    iot_integration.stop()
    
    mock_client.loop_stop.assert_called_once()
    mock_client.disconnect.assert_called_once()

def test_register_device(
    iot_integration,
    mock_client,
    test_device
):
    """Test device registration."""
    iot_integration.register_device(test_device)
    
    assert test_device.device_id in iot_integration.devices
    mock_client.subscribe.assert_called_once()
    
    # Verify subscription topics
    call_args = mock_client.subscribe.call_args[0][0]
    topics = [t[0] for t in call_args]
    assert f'devices/{test_device.device_id}/status' in topics
    assert f'devices/{test_device.device_id}/telemetry' in topics
    assert f'devices/{test_device.device_id}/events' in topics

def test_remove_device(
    iot_integration,
    mock_client,
    test_device
):
    """Test device removal."""
    # Register device first
    iot_integration.register_device(test_device)
    
    # Remove device
    iot_integration.remove_device(test_device.device_id)
    
    assert test_device.device_id not in iot_integration.devices
    mock_client.unsubscribe.assert_called_once()
    
    # Verify unsubscription topics
    call_args = mock_client.unsubscribe.call_args[0][0]
    assert f'devices/{test_device.device_id}/status' in call_args
    assert f'devices/{test_device.device_id}/telemetry' in call_args
    assert f'devices/{test_device.device_id}/events' in call_args

def test_routing_rules(
    iot_integration,
    mock_client
):
    """Test message routing rules."""
    source = 'devices/+/temperature'
    destinations = ['analytics/temp', 'storage/temp']
    
    # Add rule
    iot_integration.add_routing_rule(
        source,
        destinations
    )
    
    assert source in iot_integration.routing_rules
    assert iot_integration.routing_rules[source] == destinations
    
    # Remove rule
    iot_integration.remove_routing_rule(source)
    assert source not in iot_integration.routing_rules

def test_message_routing(
    iot_integration,
    mock_client,
    test_device
):
    """Test message routing functionality."""
    # Register device and routing rule
    iot_integration.register_device(test_device)
    iot_integration.add_routing_rule(
        'devices/+/temperature',
        ['analytics/temp']
    )
    
    # Create mock message
    message = {
        'value': 25.5,
        'timestamp': datetime.now().isoformat()
    }
    mock_mqtt_message = Mock()
    mock_mqtt_message.topic = f'devices/{test_device.device_id}/temperature'
    mock_mqtt_message.payload = json.dumps(message).encode()
    
    # Trigger message handler
    iot_integration._on_message(None, None, mock_mqtt_message)
    
    # Verify message forwarding
    mock_client.publish.assert_called_once()
    call_args = mock_client.publish.call_args[0]
    assert call_args[0] == 'analytics/temp'
    assert json.loads(call_args[1]) == message

def test_message_verification(
    iot_integration,
    test_device
):
    """Test message signature verification."""
    # Register device
    iot_integration.register_device(test_device)
    
    # Create signed message
    message = {
        'value': 25.5,
        'timestamp': datetime.now().isoformat()
    }
    message_str = json.dumps(message, sort_keys=True)
    signature = base64.b64encode(
        hmac.new(
            test_device.auth_key.encode(),
            message_str.encode(),
            hashlib.sha256
        ).digest()
    ).decode()
    
    # Verify valid signature
    assert iot_integration.verify_message(
        test_device.device_id,
        message,
        signature
    )
    
    # Verify invalid signature
    assert not iot_integration.verify_message(
        test_device.device_id,
        message,
        'invalid_signature'
    )

def test_device_status_update(
    iot_integration,
    mock_client,
    test_device
):
    """Test device status updates."""
    # Register device
    iot_integration.register_device(test_device)
    
    # Create status message
    message = {
        'status': 'online',
        'timestamp': datetime.now().isoformat()
    }
    mock_mqtt_message = Mock()
    mock_mqtt_message.topic = f'devices/{test_device.device_id}/status'
    mock_mqtt_message.payload = json.dumps(message).encode()
    
    # Trigger message handler
    iot_integration._on_message(None, None, mock_mqtt_message)
    
    # Verify status update
    assert iot_integration.devices[test_device.device_id].status == 'online'
    assert iot_integration.devices[test_device.device_id].last_seen is not None

def test_error_handling(
    iot_integration,
    mock_client
):
    """Test error handling."""
    # Test start error
    mock_client.connect.side_effect = Exception(
        "Connection failed"
    )
    
    with pytest.raises(Exception):
        iot_integration.start()
    
    # Test device registration error
    mock_client.subscribe.side_effect = Exception(
        "Subscribe failed"
    )
    
    with pytest.raises(Exception):
        iot_integration.register_device(
            Device(
                device_id='test',
                type='sensor',
                location='test',
                capabilities=[],
                auth_key='test'
            )
        ) 