"""
Event Store Tests
------------

Test cases for:
1. Event capturing
2. State reconstruction
3. Snapshot management
4. Event replay
"""

import pytest
from unittest.mock import Mock, patch, call
import json
from datetime import datetime
import uuid
from psycopg2.extras import RealDictCursor

from ..event_store import OrderEventStore, Event, EventType

@pytest.fixture
def mock_db():
    """Create mock database connection."""
    with patch('psycopg2.connect') as mock_connect:
        conn = Mock()
        mock_connect.return_value.__enter__.return_value = conn
        
        # Mock cursor context
        cursor = Mock()
        conn.cursor.return_value.__enter__.return_value = cursor
        
        yield {
            'conn': conn,
            'cursor': cursor
        }

@pytest.fixture
def mock_redis():
    """Create mock Redis client."""
    with patch('redis.Redis') as mock:
        redis = Mock()
        mock.return_value = redis
        yield redis

@pytest.fixture
def mock_kafka_producer():
    """Create mock Kafka producer."""
    with patch('kafka.KafkaProducer') as mock:
        producer = Mock()
        mock.return_value = producer
        yield producer

@pytest.fixture
def event_store(mock_db, mock_redis, mock_kafka_producer):
    """Create event store instance."""
    store = OrderEventStore(
        db_config={
            'dbname': 'test',
            'user': 'test',
            'password': 'test',
            'host': 'localhost'
        },
        redis_config={
            'host': 'localhost',
            'port': 6379
        },
        kafka_config={
            'bootstrap_servers': 'localhost:9092'
        }
    )
    yield store
    store.close()

def test_init_tables(event_store, mock_db):
    """Test database initialization."""
    # Verify table creation queries
    calls = mock_db['cursor'].execute.call_args_list
    
    # Check events table
    events_call = calls[0]
    assert 'CREATE TABLE IF NOT EXISTS events' in str(events_call.args[0])
    
    # Check snapshots table
    snapshots_call = calls[1]
    assert 'CREATE TABLE IF NOT EXISTS snapshots' in str(snapshots_call.args[0])

def test_append_event(event_store, mock_db, mock_kafka_producer):
    """Test event appending."""
    # Mock version query
    mock_db['cursor'].fetchone.return_value = [0]
    
    # Test data
    aggregate_id = str(uuid.uuid4())
    data = {
        'customer_id': 'cust123',
        'total_amount': 99.99
    }
    
    # Append event
    event = event_store.append_event(
        EventType.ORDER_CREATED,
        aggregate_id,
        data
    )
    
    # Verify event creation
    assert event.event_type == EventType.ORDER_CREATED
    assert event.aggregate_id == aggregate_id
    assert event.data == data
    assert event.version == 1
    
    # Verify database insert
    insert_call = mock_db['cursor'].execute.call_args_list[1]
    assert 'INSERT INTO events' in str(insert_call.args[0])
    
    # Verify Kafka publish
    mock_kafka_producer.send.assert_called_once()
    topic, message = mock_kafka_producer.send.call_args.args
    assert topic == 'order_events'
    assert message['event_type'] == EventType.ORDER_CREATED.value
    assert message['data'] == data

def test_get_events(event_store, mock_db):
    """Test event retrieval."""
    # Mock query results
    mock_db['cursor'].fetchall.return_value = [
        {
            'event_id': str(uuid.uuid4()),
            'event_type': EventType.ORDER_CREATED.value,
            'aggregate_id': 'order123',
            'data': json.dumps({'customer_id': 'cust123'}),
            'metadata': json.dumps({}),
            'timestamp': datetime.now(),
            'version': 1
        }
    ]
    
    # Get events
    events = event_store.get_events('order123')
    
    # Verify query
    query_call = mock_db['cursor'].execute.call_args
    assert 'SELECT * FROM events' in str(query_call.args[0])
    assert 'aggregate_id' in str(query_call.args[1])
    
    # Verify events
    assert len(events) == 1
    assert isinstance(events[0], Event)
    assert events[0].aggregate_id == 'order123'

def test_create_snapshot(event_store, mock_db):
    """Test snapshot creation."""
    # Test data
    aggregate_id = 'order123'
    state = {
        'status': 'created',
        'items': [],
        'version': 1
    }
    
    # Create snapshot
    event_store.create_snapshot(aggregate_id, state, 1)
    
    # Verify database insert
    insert_call = mock_db['cursor'].execute.call_args
    assert 'INSERT INTO snapshots' in str(insert_call.args[0])
    
    # Verify snapshot data
    params = insert_call.args[1]
    assert params[0] == aggregate_id
    assert json.loads(params[1]) == state
    assert params[2] == 1

def test_get_snapshot(event_store, mock_db):
    """Test snapshot retrieval."""
    # Mock query result
    mock_db['cursor'].fetchone.return_value = {
        'state': json.dumps({
            'status': 'created',
            'items': [],
            'version': 1
        })
    }
    
    # Get snapshot
    state = event_store.get_snapshot('order123')
    
    # Verify query
    query_call = mock_db['cursor'].execute.call_args
    assert 'SELECT * FROM snapshots' in str(query_call.args[0])
    
    # Verify state
    assert state['status'] == 'created'
    assert state['version'] == 1

def test_reconstruct_state(event_store, mock_db):
    """Test state reconstruction."""
    # Mock snapshot
    mock_db['cursor'].fetchone.return_value = {
        'state': json.dumps({
            'status': 'created',
            'items': [],
            'version': 1
        })
    }
    
    # Mock events
    mock_db['cursor'].fetchall.return_value = [
        {
            'event_id': str(uuid.uuid4()),
            'event_type': EventType.ITEM_ADDED.value,
            'aggregate_id': 'order123',
            'data': json.dumps({
                'item': {
                    'id': 'item1',
                    'name': 'Product 1',
                    'price': 49.99
                }
            }),
            'metadata': json.dumps({}),
            'timestamp': datetime.now(),
            'version': 2
        }
    ]
    
    # Reconstruct state
    state = event_store.reconstruct_state('order123')
    
    # Verify state
    assert state['status'] == 'created'
    assert len(state['items']) == 1
    assert state['items'][0]['id'] == 'item1'
    assert state['version'] == 2

def test_event_replay(event_store, mock_db):
    """Test event replay."""
    # Mock events
    events = [
        {
            'event_id': str(uuid.uuid4()),
            'event_type': EventType.ORDER_CREATED.value,
            'data': json.dumps({'customer_id': 'cust123'}),
            'version': 1
        },
        {
            'event_id': str(uuid.uuid4()),
            'event_type': EventType.PAYMENT_PROCESSED.value,
            'data': json.dumps({'status': 'success'}),
            'version': 2
        }
    ]
    mock_db['cursor'].fetchall.return_value = events
    
    # Replay events
    state = event_store.reconstruct_state('order123')
    
    # Verify state transitions
    assert state['status'] == 'paid'
    assert state['version'] == 2

def test_error_handling(event_store, mock_db):
    """Test error handling."""
    # Mock database error
    mock_db['cursor'].execute.side_effect = Exception("Database error")
    
    # Test event append
    with pytest.raises(Exception):
        event_store.append_event(
            EventType.ORDER_CREATED,
            'order123',
            {}
        )
    
    # Test state reconstruction
    with pytest.raises(Exception):
        event_store.reconstruct_state('order123')

def test_cleanup(event_store, mock_redis, mock_kafka_producer):
    """Test resource cleanup."""
    event_store.close()
    
    mock_redis.close.assert_called_once()
    mock_kafka_producer.close.assert_called_once() 