"""
Event Store Implementation
--------------------

Real Product Example: E-commerce Order System
- Captures all order-related events
- Maintains order state through event sourcing
- Enables event replay for auditing/recovery
- Supports point-in-time state reconstruction
"""

import logging
from typing import Dict, Any, List, Optional, Iterator
from datetime import datetime
import json
import uuid
from dataclasses import dataclass
from enum import Enum
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
from kafka import KafkaProducer, KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventType(Enum):
    """Order event types."""
    ORDER_CREATED = 'order_created'
    ITEM_ADDED = 'item_added'
    ITEM_REMOVED = 'item_removed'
    PAYMENT_PROCESSED = 'payment_processed'
    ORDER_FULFILLED = 'order_fulfilled'
    ORDER_CANCELLED = 'order_cancelled'

@dataclass
class Event:
    """Event structure."""
    event_id: str
    event_type: EventType
    aggregate_id: str
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    timestamp: datetime
    version: int

class OrderEventStore:
    """Order event store implementation."""
    
    def __init__(
        self,
        db_config: Dict[str, str],
        redis_config: Dict[str, Any],
        kafka_config: Dict[str, Any]
    ):
        """Initialize event store."""
        self.db_config = db_config
        self.redis = redis.Redis(**redis_config)
        
        # Kafka producer for event streaming
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode()
        )
        
        # Create tables
        self._init_tables()
    
    def _init_tables(self):
        """Initialize database tables."""
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                # Events table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        event_id VARCHAR(36) PRIMARY KEY,
                        event_type VARCHAR(50),
                        aggregate_id VARCHAR(36),
                        data JSONB,
                        metadata JSONB,
                        timestamp TIMESTAMP,
                        version INTEGER,
                        UNIQUE (aggregate_id, version)
                    )
                """)
                
                # Snapshots table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS snapshots (
                        aggregate_id VARCHAR(36),
                        state JSONB,
                        version INTEGER,
                        timestamp TIMESTAMP,
                        PRIMARY KEY (aggregate_id, version)
                    )
                """)
            
            conn.commit()
    
    def append_event(
        self,
        event_type: EventType,
        aggregate_id: str,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Event:
        """Append new event."""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # Get current version
                    cur.execute("""
                        SELECT MAX(version)
                        FROM events
                        WHERE aggregate_id = %s
                    """, (aggregate_id,))
                    
                    current_version = cur.fetchone()[0] or 0
                    new_version = current_version + 1
                    
                    # Create event
                    event = Event(
                        event_id=str(uuid.uuid4()),
                        event_type=event_type,
                        aggregate_id=aggregate_id,
                        data=data,
                        metadata=metadata or {},
                        timestamp=datetime.now(),
                        version=new_version
                    )
                    
                    # Store event
                    cur.execute("""
                        INSERT INTO events
                        (event_id, event_type, aggregate_id, data, metadata, timestamp, version)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        event.event_id,
                        event.event_type.value,
                        event.aggregate_id,
                        json.dumps(event.data),
                        json.dumps(event.metadata),
                        event.timestamp,
                        event.version
                    ))
                    
                    # Publish event
                    self.producer.send(
                        'order_events',
                        {
                            'event_id': event.event_id,
                            'event_type': event.event_type.value,
                            'aggregate_id': event.aggregate_id,
                            'data': event.data,
                            'metadata': event.metadata,
                            'timestamp': event.timestamp.isoformat(),
                            'version': event.version
                        }
                    )
                    
                    conn.commit()
                    return event
                    
        except Exception as e:
            logger.error(f"Failed to append event: {e}")
            raise
    
    def get_events(
        self,
        aggregate_id: str,
        start_version: Optional[int] = None,
        end_version: Optional[int] = None
    ) -> List[Event]:
        """Get events for aggregate."""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    query = """
                        SELECT *
                        FROM events
                        WHERE aggregate_id = %s
                    """
                    params = [aggregate_id]
                    
                    if start_version is not None:
                        query += " AND version >= %s"
                        params.append(start_version)
                    
                    if end_version is not None:
                        query += " AND version <= %s"
                        params.append(end_version)
                    
                    query += " ORDER BY version"
                    
                    cur.execute(query, params)
                    events = []
                    
                    for row in cur:
                        events.append(Event(
                            event_id=row['event_id'],
                            event_type=EventType(row['event_type']),
                            aggregate_id=row['aggregate_id'],
                            data=row['data'],
                            metadata=row['metadata'],
                            timestamp=row['timestamp'],
                            version=row['version']
                        ))
                    
                    return events
                    
        except Exception as e:
            logger.error(f"Failed to get events: {e}")
            raise
    
    def create_snapshot(
        self,
        aggregate_id: str,
        state: Dict[str, Any],
        version: int
    ):
        """Create state snapshot."""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO snapshots
                        (aggregate_id, state, version, timestamp)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        aggregate_id,
                        json.dumps(state),
                        version,
                        datetime.now()
                    ))
                    
                    conn.commit()
                    
        except Exception as e:
            logger.error(f"Failed to create snapshot: {e}")
            raise
    
    def get_snapshot(
        self,
        aggregate_id: str,
        version: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Get latest snapshot before version."""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    query = """
                        SELECT *
                        FROM snapshots
                        WHERE aggregate_id = %s
                    """
                    params = [aggregate_id]
                    
                    if version is not None:
                        query += " AND version <= %s"
                        params.append(version)
                    
                    query += " ORDER BY version DESC LIMIT 1"
                    
                    cur.execute(query, params)
                    snapshot = cur.fetchone()
                    
                    return snapshot['state'] if snapshot else None
                    
        except Exception as e:
            logger.error(f"Failed to get snapshot: {e}")
            raise
    
    def reconstruct_state(
        self,
        aggregate_id: str,
        target_version: Optional[int] = None
    ) -> Dict[str, Any]:
        """Reconstruct aggregate state."""
        try:
            # Get latest snapshot
            state = self.get_snapshot(
                aggregate_id,
                target_version
            ) or {'status': 'initialized'}
            
            # Get events since snapshot
            snapshot_version = state.get('version', 0)
            events = self.get_events(
                aggregate_id,
                start_version=snapshot_version + 1,
                end_version=target_version
            )
            
            # Apply events
            for event in events:
                if event.event_type == EventType.ORDER_CREATED:
                    state.update(event.data)
                    state['status'] = 'created'
                    
                elif event.event_type == EventType.ITEM_ADDED:
                    items = state.get('items', [])
                    items.append(event.data['item'])
                    state['items'] = items
                    
                elif event.event_type == EventType.ITEM_REMOVED:
                    items = state.get('items', [])
                    items = [
                        item for item in items
                        if item['id'] != event.data['item_id']
                    ]
                    state['items'] = items
                    
                elif event.event_type == EventType.PAYMENT_PROCESSED:
                    state['payment'] = event.data
                    state['status'] = 'paid'
                    
                elif event.event_type == EventType.ORDER_FULFILLED:
                    state['fulfillment'] = event.data
                    state['status'] = 'fulfilled'
                    
                elif event.event_type == EventType.ORDER_CANCELLED:
                    state['cancellation'] = event.data
                    state['status'] = 'cancelled'
            
            state['version'] = events[-1].version if events else snapshot_version
            return state
            
        except Exception as e:
            logger.error(f"Failed to reconstruct state: {e}")
            raise
    
    def close(self):
        """Close connections."""
        try:
            self.redis.close()
            self.producer.close()
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")

def main():
    """Run event store example."""
    # Configuration
    db_config = {
        'dbname': 'eventstore',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost'
    }
    
    redis_config = {
        'host': 'localhost',
        'port': 6379,
        'db': 0
    }
    
    kafka_config = {
        'bootstrap_servers': 'localhost:9092'
    }
    
    store = OrderEventStore(
        db_config,
        redis_config,
        kafka_config
    )
    
    try:
        # Create order
        order_id = str(uuid.uuid4())
        store.append_event(
            EventType.ORDER_CREATED,
            order_id,
            {
                'customer_id': 'cust123',
                'total_amount': 99.99
            }
        )
        
        # Add items
        store.append_event(
            EventType.ITEM_ADDED,
            order_id,
            {
                'item': {
                    'id': 'item1',
                    'name': 'Product 1',
                    'price': 49.99
                }
            }
        )
        
        # Process payment
        store.append_event(
            EventType.PAYMENT_PROCESSED,
            order_id,
            {
                'payment_id': 'pay123',
                'amount': 99.99,
                'status': 'success'
            }
        )
        
        # Create snapshot
        state = store.reconstruct_state(order_id)
        store.create_snapshot(order_id, state, state['version'])
        
        # Reconstruct state
        final_state = store.reconstruct_state(order_id)
        logger.info(f"Order state: {json.dumps(final_state, indent=2)}")
        
    finally:
        store.close()

if __name__ == '__main__':
    main() 