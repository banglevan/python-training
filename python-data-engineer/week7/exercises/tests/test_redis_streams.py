"""
Redis Streams Tests
---------------

Test cases for:
1. Stream operations
2. Consumer groups
3. Message handling
4. Error recovery
"""

import pytest
from unittest.mock import Mock, patch
import redis
import json
from datetime import datetime
import time

from ..redis_streams import RedisStreams, message_handler

@pytest.fixture
def redis_streams():
    """Create Redis streams instance."""
    with patch('redis.Redis') as mock_redis:
        streams = RedisStreams(
            host='localhost',
            port=6379,
            db=0
        )
        yield streams
        streams.close()

@pytest.fixture
def mock_redis():
    """Create mock Redis client."""
    with patch('redis.Redis') as mock:
        yield mock.return_value

def test_create_stream(
    redis_streams,
    mock_redis
):
    """Test stream creation."""
    stream = 'test_stream'
    max_length = 1000
    
    # Mock message ID
    mock_redis.xadd.return_value = '1-0'
    
    redis_streams.create_stream(
        stream,
        max_length,
        True
    )
    
    # Verify stream creation
    mock_redis.xadd.assert_called_once()
    mock_redis.xtrim.assert_called_once_with(
        stream,
        maxlen=max_length,
        approximate=True
    )
    mock_redis.xdel.assert_called_once_with(
        stream,
        '1-0'
    )

def test_add_message(
    redis_streams,
    mock_redis
):
    """Test message addition."""
    stream = 'test_stream'
    message = {
        'id': 1,
        'data': 'test'
    }
    max_length = 1000
    
    # Mock message ID
    mock_redis.xadd.return_value = '1-0'
    
    message_id = redis_streams.add_message(
        stream,
        message,
        max_length
    )
    
    assert message_id == '1-0'
    mock_redis.xadd.assert_called_once()
    call_args = mock_redis.xadd.call_args[0]
    assert call_args[0] == stream
    assert all(
        isinstance(v, str)
        for v in call_args[1].values()
    )

def test_consumer_group(
    redis_streams,
    mock_redis
):
    """Test consumer group operations."""
    stream = 'test_stream'
    group = 'test_group'
    
    # Test creation
    redis_streams.create_consumer_group(
        stream,
        group,
        '0'
    )
    
    mock_redis.xgroup_create.assert_called_once_with(
        stream,
        group,
        '0',
        mkstream=True
    )
    
    # Test existing group error
    mock_redis.xgroup_create.side_effect = (
        redis.exceptions.ResponseError('BUSYGROUP')
    )
    
    redis_streams.create_consumer_group(
        stream,
        group,
        '0'
    )

def test_read_messages(
    redis_streams,
    mock_redis
):
    """Test message reading."""
    streams = {'test_stream': '0'}
    
    # Mock messages
    mock_redis.xread.return_value = [
        ('test_stream', [
            ('1-0', {'id': '1', 'data': 'test'})
        ])
    ]
    
    messages = redis_streams.read_messages(
        streams,
        count=10,
        block=1000
    )
    
    assert 'test_stream' in messages
    assert len(messages['test_stream']) == 1
    msg_id, msg = messages['test_stream'][0]
    assert msg_id == '1-0'
    assert msg == {'id': '1', 'data': 'test'}

def test_read_group(
    redis_streams,
    mock_redis
):
    """Test consumer group reading."""
    group = 'test_group'
    consumer = 'test_consumer'
    streams = {'test_stream': '>'}
    
    # Mock messages
    mock_redis.xreadgroup.return_value = [
        ('test_stream', [
            ('1-0', {'id': '1', 'data': 'test'})
        ])
    ]
    
    messages = redis_streams.read_group(
        group,
        consumer,
        streams,
        count=10,
        block=1000
    )
    
    assert 'test_stream' in messages
    assert len(messages['test_stream']) == 1
    msg_id, msg = messages['test_stream'][0]
    assert msg_id == '1-0'
    assert msg == {'id': '1', 'data': 'test'}

def test_acknowledge_message(
    redis_streams,
    mock_redis
):
    """Test message acknowledgment."""
    stream = 'test_stream'
    group = 'test_group'
    message_id = '1-0'
    
    redis_streams.acknowledge_message(
        stream,
        group,
        message_id
    )
    
    mock_redis.xack.assert_called_once_with(
        stream,
        group,
        message_id
    )

def test_claim_pending(
    redis_streams,
    mock_redis
):
    """Test pending message claiming."""
    stream = 'test_stream'
    group = 'test_group'
    consumer = 'test_consumer'
    
    # Mock pending messages
    mock_redis.xpending.return_value = {
        'pending': 1,
        'min': '1-0',
        'max': '1-0',
        'consumers': {
            'old_consumer': 1
        }
    }
    
    mock_redis.xpending_range.return_value = [
        {
            'message_id': '1-0',
            'consumer': 'old_consumer',
            'time_since_delivered': 60000
        }
    ]
    
    mock_redis.xclaim.return_value = [
        ('1-0', {'id': '1', 'data': 'test'})
    ]
    
    messages = redis_streams.claim_pending_messages(
        stream,
        group,
        consumer,
        min_idle_time=30000
    )
    
    assert len(messages) == 1
    msg_id, msg = messages[0]
    assert msg_id == '1-0'
    assert msg == {'id': '1', 'data': 'test'}

def test_error_handling(
    redis_streams,
    mock_redis
):
    """Test error handling."""
    # Test stream creation error
    mock_redis.xadd.side_effect = redis.RedisError(
        "Stream creation failed"
    )
    
    with pytest.raises(Exception):
        redis_streams.create_stream('test_stream')
    
    # Test consumer group error
    mock_redis.xgroup_create.side_effect = Exception(
        "Group creation failed"
    )
    
    with pytest.raises(Exception):
        redis_streams.create_consumer_group(
            'test_stream',
            'test_group'
        )

def test_cleanup(
    redis_streams,
    mock_redis
):
    """Test resource cleanup."""
    redis_streams.close()
    mock_redis.close.assert_called_once() 