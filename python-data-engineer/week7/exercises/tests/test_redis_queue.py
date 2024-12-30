"""
Redis Queue Tests
-------------

Test cases for:
1. Pub/Sub operations
2. List operations
3. Message handling
4. Connection management
"""

import pytest
from unittest.mock import Mock, patch
import redis
import json
from datetime import datetime
import time

from ..redis_queue import RedisQueue, message_handler

@pytest.fixture
def redis_queue():
    """Create Redis queue instance."""
    with patch('redis.Redis') as mock_redis:
        queue = RedisQueue(
            host='localhost',
            port=6379,
            db=0
        )
        yield queue
        queue.close()

@pytest.fixture
def mock_redis():
    """Create mock Redis client."""
    with patch('redis.Redis') as mock:
        yield mock.return_value

@pytest.fixture
def mock_pubsub():
    """Create mock PubSub object."""
    mock = Mock()
    mock.get_message.return_value = None
    return mock

def test_publish(
    redis_queue,
    mock_redis
):
    """Test message publishing."""
    channel = 'test_channel'
    message = {
        'id': 1,
        'data': 'test'
    }
    
    redis_queue.publish(channel, message)
    
    mock_redis.publish.assert_called_once_with(
        channel,
        json.dumps(message)
    )

def test_subscribe(
    redis_queue,
    mock_redis,
    mock_pubsub
):
    """Test channel subscription."""
    channels = ['test_channel', 'alerts.*']
    callback = Mock()
    
    # Setup mock pubsub
    mock_redis.pubsub.return_value = mock_pubsub
    redis_queue.pubsub = mock_pubsub
    
    # Subscribe
    redis_queue.subscribe(channels, callback, True)
    
    # Verify subscription
    mock_pubsub.psubscribe.assert_called_once()
    subscription_dict = mock_pubsub.psubscribe.call_args[1]
    assert all(ch in subscription_dict for ch in channels)

def test_message_handler(
    redis_queue
):
    """Test message handling."""
    message = {
        'channel': 'test_channel',
        'data': json.dumps({
            'id': 1,
            'data': 'test'
        }),
        'type': 'message'
    }
    
    # Process message
    redis_queue._message_handler(message)
    
    # Verify message in buffer
    buffered = redis_queue.message_buffer.get_nowait()
    assert buffered == message

def test_list_operations(
    redis_queue,
    mock_redis
):
    """Test list push/pop operations."""
    key = 'test_list'
    value = {
        'id': 1,
        'data': 'test'
    }
    
    # Test push
    redis_queue.list_push(key, value)
    mock_redis.lpush.assert_called_once_with(
        key,
        json.dumps(value)
    )
    
    # Test pop
    mock_redis.rpop.return_value = json.dumps(value)
    result = redis_queue.list_pop(key)
    assert result == value
    
    # Test blocking pop
    mock_redis.brpop.return_value = (key, json.dumps(value))
    result = redis_queue.list_pop(key, timeout=1)
    assert result == value

def test_unsubscribe(
    redis_queue,
    mock_redis,
    mock_pubsub
):
    """Test channel unsubscription."""
    channels = ['test_channel']
    
    # Setup mock pubsub
    mock_redis.pubsub.return_value = mock_pubsub
    redis_queue.pubsub = mock_pubsub
    
    # Subscribe then unsubscribe
    redis_queue.subscribe(channels, Mock())
    redis_queue.unsubscribe(channels)
    
    mock_pubsub.unsubscribe.assert_called_once_with(
        channels
    )

def test_error_handling(
    redis_queue,
    mock_redis
):
    """Test error handling."""
    # Test publish error
    mock_redis.publish.side_effect = redis.RedisError(
        "Publish failed"
    )
    
    with pytest.raises(Exception):
        redis_queue.publish(
            'test_channel',
            {'test': 'data'}
        )
    
    # Test list operation error
    mock_redis.lpush.side_effect = redis.RedisError(
        "Push failed"
    )
    
    with pytest.raises(Exception):
        redis_queue.list_push(
            'test_list',
            {'test': 'data'}
        )

def test_cleanup(
    redis_queue,
    mock_redis,
    mock_pubsub
):
    """Test resource cleanup."""
    # Setup mock pubsub
    mock_redis.pubsub.return_value = mock_pubsub
    redis_queue.pubsub = mock_pubsub
    
    redis_queue.close()
    
    mock_pubsub.close.assert_called_once()
    mock_redis.close.assert_called_once() 