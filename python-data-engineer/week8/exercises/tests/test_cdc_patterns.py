"""
CDC Patterns Tests
--------------

Test cases for:
1. Change detection
2. Data versioning
3. Integrity verification
"""

import pytest
from unittest.mock import Mock, patch
import json
from datetime import datetime
from psycopg2.extras import DictRow

from ..cdc_patterns import (
    CDCPatterns,
    Change,
    ChangeType
)

@pytest.fixture
def mock_db_conn():
    """Create mock database connection."""
    with patch('psycopg2.connect') as mock:
        conn = Mock()
        mock.return_value = conn
        yield conn

@pytest.fixture
def mock_redis():
    """Create mock Redis client."""
    with patch('redis.Redis') as mock:
        redis = Mock()
        mock.return_value = redis
        yield redis

@pytest.fixture
def cdc_handler(mock_db_conn, mock_redis):
    """Create CDC handler instance."""
    config = {
        'db_config': {
            'dbname': 'test',
            'user': 'test',
            'password': 'test',
            'host': 'localhost'
        },
        'redis_config': {
            'host': 'localhost',
            'port': 6379
        }
    }
    
    handler = CDCPatterns(**config)
    yield handler
    handler.close()

def test_init_tables(cdc_handler, mock_db_conn):
    """Test table initialization."""
    cursor = Mock()
    mock_db_conn.cursor.return_value.__enter__.return_value = cursor
    
    cdc_handler._init_tables()
    
    # Verify table creation
    assert cursor.execute.call_count == 2
    create_table_call = cursor.execute.call_args_list[0]
    assert 'CREATE TABLE IF NOT EXISTS change_log' in create_table_call[0][0]

def test_enable_cdc(cdc_handler, mock_db_conn):
    """Test enabling CDC for a table."""
    cursor = Mock()
    mock_db_conn.cursor.return_value.__enter__.return_value = cursor
    
    cdc_handler.enable_cdc('test_table', ['id'])
    
    # Verify trigger creation
    assert cursor.execute.call_count == 2
    create_trigger_call = cursor.execute.call_args_list[1]
    assert 'CREATE TRIGGER test_table_changes' in create_trigger_call[0][0]

def test_get_changes(cdc_handler, mock_db_conn):
    """Test retrieving changes."""
    cursor = Mock()
    mock_db_conn.cursor.return_value.__enter__.return_value = cursor
    
    # Mock query results
    cursor.fetchall.return_value = [
        {
            'table_name': 'test_table',
            'primary_key': {'id': 1},
            'change_type': 'INSERT',
            'old_data': None,
            'new_data': {'id': 1, 'name': 'test'},
            'timestamp': datetime.now(),
            'version': 1,
            'checksum': 'abc123'
        }
    ]
    
    changes = cdc_handler.get_changes(
        'test_table',
        since_version=0
    )
    
    assert len(changes) == 1
    assert isinstance(changes[0], Change)
    assert changes[0].table == 'test_table'
    assert changes[0].change_type == ChangeType.INSERT

def test_verify_changes(cdc_handler):
    """Test change verification."""
    # Create test changes
    changes = [
        Change(
            table='test_table',
            primary_key={'id': 1},
            change_type=ChangeType.INSERT,
            old_data=None,
            new_data={'id': 1, 'name': 'test'},
            timestamp=datetime.now(),
            version=1,
            checksum='a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3'
        )
    ]
    
    results = cdc_handler.verify_changes(changes)
    
    assert len(results) == 1
    change, is_valid = results[0]
    assert isinstance(is_valid, bool)

def test_error_handling(cdc_handler, mock_db_conn):
    """Test error handling."""
    cursor = Mock()
    mock_db_conn.cursor.return_value.__enter__.return_value = cursor
    cursor.execute.side_effect = Exception("Database error")
    
    with pytest.raises(Exception):
        cdc_handler.enable_cdc('test_table', ['id']) 