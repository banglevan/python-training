"""
Unit tests for event producers.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch
from src.utils.config import Config
from src.ingestion.producers.event_producer import EventProducer, Event

@pytest.fixture
def mock_config():
    """Mock configuration."""
    config = Mock(spec=Config)
    config.kafka.bootstrap_servers = ['localhost:9092']
    config.kafka.topics = {
        'events': {
            'name': 'test_events'
        }
    }
    return config

@pytest.fixture
def producer(mock_config):
    """Test producer instance."""
    with patch('kafka.KafkaProducer'):
        return EventProducer(mock_config)

def test_generate_event(producer):
    """Test event generation."""
    event = producer.generate_event()
    
    assert isinstance(event, Event)
    assert event.event_id.startswith('evt_')
    assert event.event_type in ['view', 'cart', 'purchase']
    assert 1 <= event.user_id <= 1000
    assert event.product_id.startswith('product_')
    assert 1 <= event.quantity <= 5
    assert 10 <= event.price <= 1000
    assert isinstance(event.timestamp, str)
    assert event.session_id.startswith('session_')

def test_send_event(producer):
    """Test event sending."""
    event = producer.generate_event()
    
    # Mock successful send
    future = Mock()
    future.get.return_value = None
    producer.producer.send.return_value = future
    
    producer.send_event(event)
    
    producer.producer.send.assert_called_once_with(
        'test_events',
        key=event.event_id,
        value=event.to_dict()
    )

def test_send_event_failure(producer):
    """Test event sending failure."""
    event = producer.generate_event()
    
    # Mock failed send
    future = Mock()
    future.get.side_effect = Exception("Send failed")
    producer.producer.send.return_value = future
    
    with pytest.raises(Exception):
        producer.send_event(event) 