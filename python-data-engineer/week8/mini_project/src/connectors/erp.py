"""
ERP connector implementation using Debezium.
"""

import logging
from typing import Dict, Any, List, Optional, Iterator
from datetime import datetime
import hashlib
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from debezium import Debezium

from .base import DataConnector, SourceEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ERPConnector(DataConnector):
    """ERP system connector using Debezium."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize ERP connector."""
        super().__init__(config)
        self.db_config = config['database']
        self.tables = config.get('tables', ['products', 'orders'])
        self.debezium = None
        self.snapshot_mode = config.get('snapshot_mode', 'initial')
        self._event_queue = []
    
    def initialize(self) -> bool:
        """Initialize Debezium connection."""
        try:
            # Configure Debezium connector
            self.debezium = Debezium({
                'connector.class': 
                    'io.debezium.connector.postgresql.PostgresConnector',
                'database.hostname': self.db_config['host'],
                'database.port': self.db_config['port'],
                'database.user': self.db_config['user'],
                'database.password': self.db_config['password'],
                'database.dbname': self.db_config['dbname'],
                'database.server.name': self.config.get('server_name', 'erp'),
                'table.include.list': ','.join(self.tables),
                'plugin.name': 'pgoutput',
                'snapshot.mode': self.snapshot_mode,
                'tombstones.on.delete': 'false',
                'decimal.handling.mode': 'string'
            })
            
            # Register event handlers
            self.debezium.on_message(self._handle_event)
            self.debezium.on_error(self._handle_error)
            
            # Start connector
            return self.debezium.start()
            
        except Exception as e:
            logger.error(f"ERP initialization failed: {e}")
            return False
    
    def start(self) -> bool:
        """Start the connector."""
        try:
            if not self.debezium:
                return self.initialize()
            
            if not self.debezium.is_running():
                return self.debezium.start()
            
            return True
            
        except Exception as e:
            logger.error(f"Start failed: {e}")
            return False
    
    def stop(self) -> bool:
        """Stop the connector."""
        try:
            if self.debezium and self.debezium.is_running():
                self.debezium.stop()
            return True
            
        except Exception as e:
            logger.error(f"Stop failed: {e}")
            return False
    
    def get_events(self) -> Iterator[SourceEvent]:
        """Get captured events from ERP."""
        try:
            while self._event_queue:
                event_data = self._event_queue.pop(0)
                yield self._create_event(event_data)
                
        except Exception as e:
            logger.error(f"Event processing failed: {e}")
            raise
    
    def _handle_event(self, event: Dict[str, Any]):
        """Handle incoming Debezium event."""
        try:
            if not event or 'payload' not in event:
                return
            
            payload = event['payload']
            
            # Skip schema events
            if 'schema' in payload and not payload.get('before') and not payload.get('after'):
                return
            
            self._event_queue.append({
                'table': payload['source']['table'],
                'operation': payload['op'],
                'before': payload.get('before'),
                'after': payload.get('after'),
                'timestamp': payload['source']['ts_ms']
            })
            
        except Exception as e:
            logger.error(f"Event handling failed: {e}")
    
    def _handle_error(self, error: Exception):
        """Handle Debezium error."""
        logger.error(f"Debezium error: {error}")
    
    def _create_event(self, data: Dict[str, Any]) -> SourceEvent:
        """Create source event from Debezium data."""
        event_id = hashlib.sha256(
            f"{data['table']}:{data['timestamp']}".encode()
        ).hexdigest()
        
        return SourceEvent(
            event_id=event_id,
            source_type='ERP',
            source_id=self.config.get('server_name', 'erp'),
            event_type=f"{data['table']}.{data['operation']}",
            data={
                'table': data['table'],
                'operation': data['operation'],
                'before': data['before'],
                'after': data['after']
            },
            timestamp=datetime.fromtimestamp(data['timestamp'] / 1000),
            checksum=self._generate_checksum(data)
        )
    
    def _generate_checksum(self, data: Dict[str, Any]) -> str:
        """Generate checksum for data."""
        return hashlib.sha256(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()
    
    def health_check(self) -> Dict[str, Any]:
        """Check connector health."""
        try:
            status = {
                'name': self.name,
                'type': 'ERP',
                'connected': False,
                'tables_monitored': self.tables,
                'last_check': datetime.now().isoformat()
            }
            
            if self.debezium:
                status['connected'] = self.debezium.is_running()
                
                # Check database connection
                with psycopg2.connect(**self.db_config) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        status['db_connected'] = True
                
                # Get connector metrics
                metrics = self.debezium.get_metrics()
                if metrics:
                    status.update({
                        'events_processed': metrics.get('total_events', 0),
                        'last_event_timestamp': metrics.get('last_event_timestamp'),
                        'snapshot_completed': metrics.get('snapshot_completed', False)
                    })
            
            return status
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                'name': self.name,
                'type': 'ERP',
                'connected': False,
                'error': str(e),
                'last_check': datetime.now().isoformat()
            }
    
    def acknowledge_event(self, event_id: str) -> bool:
        """Acknowledge event processing."""
        # Debezium handles offsets automatically
        return True 