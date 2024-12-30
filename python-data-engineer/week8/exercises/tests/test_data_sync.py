"""
Data Sync Tests
-----------

Test cases for:
1. Product synchronization
2. Conflict resolution
3. Recovery strategies
"""

import pytest
from unittest.mock import Mock, patch, call
import json
from datetime import datetime, timedelta
import requests
from sqlalchemy import create_engine, text

from ..data_sync import ShopifySync, SyncEvent

@pytest.fixture
def mock_requests():
    """Create mock requests."""
    with patch('requests.get') as mock_get, \
         patch('requests.put') as mock_put:
        yield {
            'get': mock_get,
            'put': mock_put
        }

@pytest.fixture
def mock_db():
    """Create mock database."""
    with patch('sqlalchemy.create_engine') as mock_engine:
        engine = Mock()
        mock_engine.return_value = engine
        
        # Mock connection context
        conn = Mock()
        engine.connect.return_value.__enter__.return_value = conn
        
        yield {
            'engine': engine,
            'conn': conn
        }

@pytest.fixture
def mock_redis():
    """Create mock Redis."""
    with patch('redis.Redis') as mock:
        redis = Mock()
        mock.from_url.return_value = redis
        yield redis

@pytest.fixture
def sync_manager(mock_db, mock_redis):
    """Create sync manager instance."""
    stores_config = {
        'store1': {
            'shop_url': 'store1.myshopify.com',
            'access_token': 'token1'
        },
        'store2': {
            'shop_url': 'store2.myshopify.com',
            'access_token': 'token2'
        }
    }
    
    sync = ShopifySync(
        stores_config,
        'postgresql://test:test@localhost/test',
        'redis://localhost'
    )
    yield sync
    sync.close()

def test_init_tables(sync_manager, mock_db):
    """Test database initialization."""
    # Verify table creation queries
    calls = mock_db['conn'].execute.call_args_list
    
    # Check sync_events table
    sync_events_call = calls[0]
    assert 'CREATE TABLE IF NOT EXISTS sync_events' in \
        str(sync_events_call.args[0])
    
    # Check conflict_log table
    conflict_log_call = calls[1]
    assert 'CREATE TABLE IF NOT EXISTS conflict_log' in \
        str(conflict_log_call.args[0])

def test_fetch_product(sync_manager, mock_requests):
    """Test product fetching."""
    # Mock successful response
    mock_requests['get'].return_value.status_code = 200
    mock_requests['get'].return_value.json.return_value = {
        'product': {
            'id': '123',
            'title': 'Test Product',
            'variants': []
        }
    }
    
    # Test fetch
    product = sync_manager._fetch_product('store1', '123')
    
    assert product is not None
    assert product['id'] == '123'
    assert product['title'] == 'Test Product'
    
    # Verify request
    mock_requests['get'].assert_called_with(
        'https://store1.myshopify.com/admin/api/2024-01/products/123.json',
        headers={'X-Shopify-Access-Token': 'token1'}
    )
    
    # Test error handling
    mock_requests['get'].return_value.status_code = 404
    product = sync_manager._fetch_product('store1', '123')
    assert product is None

def test_update_product(sync_manager, mock_requests):
    """Test product updating."""
    # Mock successful response
    mock_requests['put'].return_value.status_code = 200
    
    # Test update
    data = {
        'title': 'Updated Product',
        'price': '19.99'
    }
    
    result = sync_manager._update_product('store1', '123', data)
    assert result == True
    
    # Verify request
    mock_requests['put'].assert_called_with(
        'https://store1.myshopify.com/admin/api/2024-01/products/123.json',
        headers={
            'X-Shopify-Access-Token': 'token1',
            'Content-Type': 'application/json'
        },
        json={'product': data}
    )
    
    # Test error handling
    mock_requests['put'].return_value.status_code = 400
    result = sync_manager._update_product('store1', '123', data)
    assert result == False

def test_sync_product(sync_manager, mock_requests, mock_db):
    """Test product synchronization."""
    # Mock product fetch
    mock_requests['get'].return_value.status_code = 200
    mock_requests['get'].return_value.json.return_value = {
        'product': {
            'id': '123',
            'title': 'Test Product',
            'updated_at': datetime.now().isoformat()
        }
    }
    
    # Mock product update
    mock_requests['put'].return_value.status_code = 200
    
    # Mock database operations
    mock_db['conn'].execute.return_value.scalar_one.return_value = 1
    
    # Test sync
    sync_manager.sync_product('store1', '123', ['store2'])
    
    # Verify sync event creation
    insert_call = mock_db['conn'].execute.call_args_list[2]
    assert 'INSERT INTO sync_events' in str(insert_call.args[0])

def test_conflict_resolution(sync_manager, mock_requests, mock_db):
    """Test conflict resolution."""
    # Mock source product
    source_time = datetime.now() - timedelta(hours=1)
    mock_requests['get'].side_effect = [
        # Source product
        Mock(
            status_code=200,
            json=lambda: {
                'product': {
                    'id': '123',
                    'title': 'Source Product',
                    'updated_at': source_time.isoformat()
                }
            }
        ),
        # Target product (newer)
        Mock(
            status_code=200,
            json=lambda: {
                'product': {
                    'id': '123',
                    'title': 'Target Product',
                    'updated_at': datetime.now().isoformat()
                }
            }
        )
    ]
    
    # Mock database operations
    mock_db['conn'].execute.return_value.scalar_one.return_value = 1
    
    # Test sync with conflict
    sync_manager.sync_product('store1', '123', ['store2'])
    
    # Verify conflict log
    conflict_call = mock_db['conn'].execute.call_args_list[-2]
    assert 'INSERT INTO conflict_log' in str(conflict_call.args[0])
    assert 'timestamp_conflict' in str(conflict_call.args[1])

def test_retry_failed_syncs(sync_manager, mock_requests, mock_db):
    """Test retry mechanism."""
    # Mock failed events query
    mock_db['conn'].execute.return_value = [
        Mock(
            store_id='store1',
            product_id='123',
            conflict_type='sync_error'
        )
    ]
    
    # Mock product fetch/update for retry
    mock_requests['get'].return_value.status_code = 200
    mock_requests['get'].return_value.json.return_value = {
        'product': {
            'id': '123',
            'title': 'Test Product',
            'updated_at': datetime.now().isoformat()
        }
    }
    mock_requests['put'].return_value.status_code = 200
    
    # Test retry
    sync_manager.retry_failed_syncs()
    
    # Verify retry attempt
    assert mock_requests['get'].called
    assert mock_requests['put'].called

def test_error_handling(sync_manager, mock_requests, mock_db):
    """Test error handling."""
    # Mock network error
    mock_requests['get'].side_effect = requests.exceptions.RequestException
    
    # Test sync with error
    with pytest.raises(Exception):
        sync_manager.sync_product('store1', '123')
    
    # Verify error logging
    error_call = mock_db['conn'].execute.call_args_list[-1]
    assert 'conflict_log' in str(error_call.args[0])
    assert 'sync_error' in str(error_call.args[1])

def test_cleanup(sync_manager, mock_redis):
    """Test resource cleanup."""
    sync_manager.close()
    mock_redis.close.assert_called_once() 