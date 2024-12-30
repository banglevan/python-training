"""
Storage Tests
---------

Test cases for data storage components.
"""

import pytest
from unittest.mock import Mock, patch
import json
from datetime import datetime

from pipeline.storage import DataStorage

@pytest.fixture
def mock_db_engine():
    """Create mock database engine."""
    with patch('sqlalchemy.create_engine') as mock:
        yield mock.return_value

@pytest.fixture
def mock_redis():
    """Create mock Redis client."""
    with patch('redis.Redis') as mock:
        yield mock.return_value

@pytest.fixture
def storage(mock_db_engine, mock_redis):
    """Create storage instance."""
    storage = DataStorage(
        db_url='postgresql://localhost/test',
        redis_host='localhost',
        redis_port=6379
    )
    yield storage
    storage.close()

def test_store_measurement(storage, mock_db_engine):
    """Test measurement storage."""
    # Test data
    data = {
        'sensor_id': 'test',
        'metric': 'temperature',
        'value': 25.5,
        'timestamp': datetime.now().isoformat()
    }
    
    # Mock database connection
    mock_conn = Mock()
    mock_db_engine.connect.return_value.__enter__.return_value = mock_conn
    
    # Store measurement
    storage.store_measurement(data)
    
    # Verify database operation
    mock_conn.execute.assert_called_once()

def test_cache_data(storage, mock_redis):
    """Test data caching."""
    # Test data
    key = 'test_key'
    data = {
        'value': 25.5,
        'timestamp': datetime.now().isoformat()
    }
    expiry = 3600
    
    # Cache data
    storage.cache_data(key, data, expiry)
    
    # Verify Redis operation
    mock_redis.setex.assert_called_once_with(
        key,
        expiry,
        json.dumps(data)
    )

def test_storage_errors(storage, mock_db_engine, mock_redis):
    """Test error handling."""
    # Test database error
    mock_db_engine.connect.side_effect = Exception("DB Error")
    
    with pytest.raises(Exception):
        storage.store_measurement({'test': 'data'})
    
    # Test Redis error
    mock_redis.setex.side_effect = Exception("Redis Error")
    
    with pytest.raises(Exception):
        storage.cache_data('key', {'test': 'data'})

def test_close_connections(storage, mock_redis):
    """Test connection cleanup."""
    storage.close()
    mock_redis.close.assert_called_once() 