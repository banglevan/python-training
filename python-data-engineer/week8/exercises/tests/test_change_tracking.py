"""
Change Tracking Tests
----------------

Test cases for:
1. Audit logging
2. Version history
3. Change operations
"""

import pytest
from unittest.mock import Mock, patch
import json
from datetime import datetime
from psycopg2.extras import DictRow

from ..change_tracking import (
    ChangeTracking,
    OperationType,
    AuditLog
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
def tracker(mock_db_conn, mock_redis):
    """Create change tracker instance."""
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
    
    tracker = ChangeTracking(**config)
    yield tracker
    tracker.close()

def test_init_tables(tracker, mock_db_conn):
    """Test table initialization."""
    cursor = Mock()
    mock_db_conn.cursor.return_value.__enter__.return_value = cursor
    
    tracker._init_tables()
    
    # Verify table creation
    assert cursor.execute.call_count == 2
    create_audit_call = cursor.execute.call_args_list[0]
    assert 'CREATE TABLE IF NOT EXISTS audit_log' in create_audit_call[0][0]

def test_track_change(tracker, mock_db_conn):
    """Test change tracking."""
    cursor = Mock()
    mock_db_conn.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchone.return_value = [1]  # Mock log ID
    
    log_id = tracker.track_change(
        user_id='test_user',
        operation=OperationType.CREATE,
        table_name='test_table',
        record_id=1,
        changes={'name': 'test'}
    )
    
    assert log_id == 1
    assert cursor.execute.call_count >= 2  # Audit log + version

def test_get_audit_logs(tracker, mock_db_conn):
    """Test retrieving audit logs."""
    cursor = Mock()
    mock_db_conn.cursor.return_value.__enter__.return_value = cursor
    
    # Mock query results
    cursor.fetchall.return_value = [
        {
            'id': 1,
            'user_id': 'test_user',
            'operation': 'CREATE',
            'table_name': 'test_table',
            'record_id': '1',
            'changes': {'name': 'test'},
            'timestamp': datetime.now(),
            'metadata': None
        }
    ]
    
    logs = tracker.get_audit_logs(
        table_name='test_table',
        record_id=1
    )
    
    assert len(logs) == 1
    assert isinstance(logs[0], AuditLog)
    assert logs[0].user_id == 'test_user'
    assert logs[0].operation == OperationType.CREATE

def test_version_history(tracker, mock_db_conn):
    """Test version history management."""
    cursor = Mock()
    mock_db_conn.cursor.return_value.__enter__.return_value = cursor
    
    # Mock version query
    cursor.fetchone.return_value = [1]  # Version number
    
    # Store version
    tracker._store_version(
        cursor,
        'test_table',
        1,
        {'name': 'test'},
        'test_user'
    )
    
    assert cursor.execute.call_count == 2  # Version query + insert

def test_get_version_history(tracker, mock_db_conn):
    """Test retrieving version history."""
    cursor = Mock()
    mock_db_conn.cursor.return_value.__enter__.return_value = cursor
    
    # Mock query results
    cursor.fetchall.return_value = [
        {
            'version': 1,
            'data': {'name': 'test'},
            'created_at': datetime.now(),
            'created_by': 'test_user'
        }
    ]
    
    versions = tracker.get_version_history(
        'test_table',
        1
    )
    
    assert len(versions) == 1
    assert versions[0]['version'] == 1

def test_error_handling(tracker, mock_db_conn):
    """Test error handling."""
    cursor = Mock()
    mock_db_conn.cursor.return_value.__enter__.return_value = cursor
    cursor.execute.side_effect = Exception("Database error")
    
    with pytest.raises(Exception):
        tracker.track_change(
            user_id='test_user',
            operation=OperationType.CREATE,
            table_name='test_table',
            record_id=1,
            changes={'name': 'test'}
        ) 