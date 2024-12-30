"""
Pulsar Messaging Tests
-------------------

Test cases for:
1. Multi-tenancy
2. Message operations
3. Geo-replication
4. Consumer handling
"""

import pytest
from unittest.mock import Mock, patch
import pulsar
import json
from datetime import datetime

from ..pulsar_messaging import PulsarMessaging, message_handler

@pytest.fixture
def pulsar_messaging():
    """Create Pulsar messaging instance."""
    with patch('pulsar.Client'), \
         patch('pulsar.admin.AdminClient'):
        messaging = PulsarMessaging(
            service_url='pulsar://localhost:6650',
            admin_url='http://localhost:8080'
        )
        yield messaging
        messaging.close()

@pytest.fixture
def mock_admin():
    """Create mock admin client."""
    with patch('pulsar.admin.AdminClient') as mock:
        yield mock.return_value

@pytest.fixture
def mock_client():
    """Create mock Pulsar client."""
    with patch('pulsar.Client') as mock:
        yield mock.return_value

def test_create_tenant(
    pulsar_messaging,
    mock_admin
):
    """Test tenant creation."""
    tenant = 'test_tenant'
    clusters = ['standalone']
    
    # Mock tenants admin
    mock_tenants = Mock()
    mock_admin.tenants.return_value = mock_tenants
    
    pulsar_messaging.create_tenant(tenant, clusters)
    
    mock_tenants.create_tenant.assert_called_once_with(
        tenant,
        {
            'allowed_clusters': clusters,
            'admin_roles': []
        }
    )

def test_create_namespace(
    pulsar_messaging,
    mock_admin
):
    """Test namespace creation."""
    tenant = 'test_tenant'
    namespace = 'test_ns'
    policies = {
        'retention': {
            'size_in_mb': 100,
            'time_in_minutes': 1440
        },
        'clusters': ['standalone']
    }
    
    # Mock namespaces admin
    mock_namespaces = Mock()
    mock_admin.namespaces.return_value = mock_namespaces
    
    pulsar_messaging.create_namespace(
        tenant,
        namespace,
        policies
    )
    
    # Verify namespace creation
    mock_namespaces.create_namespace.assert_called_once_with(
        f"{tenant}/{namespace}"
    )
    
    # Verify policy settings
    mock_namespaces.set_retention.assert_called_once_with(
        f"{tenant}/{namespace}",
        policies['retention']
    )
    mock_namespaces.set_clusters.assert_called_once_with(
        f"{tenant}/{namespace}",
        policies['clusters']
    )

def test_get_producer(
    pulsar_messaging,
    mock_client
):
    """Test producer creation."""
    topic = 'test_topic'
    
    producer = pulsar_messaging.get_producer(topic)
    
    # Verify producer creation
    mock_client.create_producer.assert_called_once()
    call_args = mock_client.create_producer.call_args[0]
    
    assert call_args[0] == (
        f"persistent://public/default/{topic}"
    )
    
    # Verify producer caching
    assert topic in pulsar_messaging.producers
    assert producer == pulsar_messaging.producers[topic]

def test_get_consumer(
    pulsar_messaging,
    mock_client
):
    """Test consumer creation."""
    topic = 'test_topic'
    subscription = 'test_sub'
    
    consumer = pulsar_messaging.get_consumer(
        topic,
        subscription,
        'Shared'
    )
    
    # Verify consumer creation
    mock_client.subscribe.assert_called_once()
    call_args = mock_client.subscribe.call_args[1]
    
    assert call_args['subscription_name'] == subscription
    assert call_args['subscription_type'] == (
        pulsar.ConsumerType.Shared
    )
    
    # Verify consumer caching
    consumer_key = f"{topic}_{subscription}"
    assert consumer_key in pulsar_messaging.consumers
    assert consumer == pulsar_messaging.consumers[consumer_key]

def test_send_message(
    pulsar_messaging,
    mock_client
):
    """Test message sending."""
    topic = 'test_topic'
    message = {
        'id': 1,
        'data': 'test'
    }
    key = 'test_key'
    properties = {'source': 'test'}
    
    # Mock producer
    mock_producer = Mock()
    mock_client.create_producer.return_value = mock_producer
    
    pulsar_messaging.send_message(
        topic,
        message,
        key,
        properties
    )
    
    # Verify message sending
    mock_producer.send.assert_called_once()
    call_args = mock_producer.send.call_args[1]
    
    assert json.loads(
        call_args['data'].decode()
    ) == message
    assert call_args['partition_key'] == key
    assert call_args['properties'] == properties

def test_consume_messages(
    pulsar_messaging,
    mock_client
):
    """Test message consumption."""
    topic = 'test_topic'
    subscription = 'test_sub'
    
    # Mock consumer
    mock_consumer = Mock()
    mock_client.subscribe.return_value = mock_consumer
    
    # Mock message
    mock_message = Mock()
    mock_message.data.return_value = json.dumps({
        'test': 'data'
    }).encode()
    mock_message.properties.return_value = {
        'source': 'test'
    }
    
    # Mock receive sequence
    mock_consumer.receive.side_effect = [
        mock_message,
        KeyboardInterrupt
    ]
    
    # Test message handler
    handler_called = False
    def test_handler(msg, props):
        nonlocal handler_called
        handler_called = True
    
    pulsar_messaging.consume_messages(
        topic,
        subscription,
        test_handler
    )
    
    assert handler_called
    mock_consumer.acknowledge.assert_called_once_with(
        mock_message
    )

def test_setup_geo_replication(
    pulsar_messaging,
    mock_admin
):
    """Test geo-replication setup."""
    tenant = 'test_tenant'
    namespace = 'test_ns'
    clusters = ['primary', 'secondary']
    
    # Mock namespaces admin
    mock_namespaces = Mock()
    mock_admin.namespaces.return_value = mock_namespaces
    
    pulsar_messaging.setup_geo_replication(
        tenant,
        namespace,
        clusters
    )
    
    # Verify replication setup
    mock_namespaces.set_clusters.assert_called_once_with(
        f"{tenant}/{namespace}",
        clusters
    )
    
    mock_namespaces.set_replication_policy.assert_called_once_with(
        f"{tenant}/{namespace}",
        {
            'replication_clusters': clusters,
            'cleanup_delay_minutes': 10
        }
    )

def test_error_handling(
    pulsar_messaging,
    mock_admin,
    mock_client
):
    """Test error handling."""
    # Test tenant creation error
    mock_tenants = Mock()
    mock_admin.tenants.return_value = mock_tenants
    mock_tenants.create_tenant.side_effect = Exception(
        "Tenant creation failed"
    )
    
    with pytest.raises(Exception):
        pulsar_messaging.create_tenant(
            'test_tenant',
            ['standalone']
        )
    
    # Test message send error
    mock_producer = Mock()
    mock_client.create_producer.return_value = mock_producer
    mock_producer.send.side_effect = Exception(
        "Message send failed"
    )
    
    with pytest.raises(Exception):
        pulsar_messaging.send_message(
            'test_topic',
            {'test': 'data'}
        ) 