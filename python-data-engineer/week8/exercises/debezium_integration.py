"""
Debezium Integration
----------------

Practice with:
1. Connector setup
2. Event streaming
3. Schema evolution
4. Error handling
"""

import logging
from typing import Dict, Any, Optional, List, Callable
import json
import time
from datetime import datetime
import requests
from kafka import KafkaConsumer, KafkaProducer
from confluent_kafka.admin import AdminClient, NewTopic
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DebeziumIntegration:
    """Debezium connector integration."""
    
    def __init__(
        self,
        connect_url: str,
        kafka_config: Dict[str, Any],
        db_config: Dict[str, Any]
    ):
        """Initialize Debezium integration."""
        self.connect_url = connect_url
        self.kafka_config = kafka_config
        self.db_config = db_config
        
        # Kafka admin client
        self.admin = AdminClient(kafka_config)
        
        # Kafka consumer for CDC events
        self.consumer = KafkaConsumer(
            bootstrap_servers=kafka_config['bootstrap.servers'],
            value_deserializer=lambda v: json.loads(v.decode()),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='debezium_handler'
        )
    
    def create_connector(
        self,
        name: str,
        config: Dict[str, Any]
    ) -> bool:
        """Create Debezium connector."""
        try:
            # Set connector class
            config['connector.class'] = (
                'io.debezium.connector.postgresql.PostgresConnector'
            )
            
            # Create connector
            response = requests.post(
                f"{self.connect_url}/connectors",
                headers={'Content-Type': 'application/json'},
                data=json.dumps({
                    'name': name,
                    'config': config
                })
            )
            
            if response.status_code == 201:
                logger.info(f"Connector {name} created successfully")
                return True
            else:
                logger.error(
                    f"Failed to create connector: {response.text}"
                )
                return False
                
        except Exception as e:
            logger.error(f"Connector creation failed: {e}")
            return False
    
    def delete_connector(self, name: str) -> bool:
        """Delete Debezium connector."""
        try:
            response = requests.delete(
                f"{self.connect_url}/connectors/{name}"
            )
            
            return response.status_code == 204
            
        except Exception as e:
            logger.error(f"Connector deletion failed: {e}")
            return False
    
    def get_connector_status(
        self,
        name: str
    ) -> Optional[Dict[str, Any]]:
        """Get connector status."""
        try:
            response = requests.get(
                f"{self.connect_url}/connectors/{name}/status"
            )
            
            if response.status_code == 200:
                return response.json()
            return None
            
        except Exception as e:
            logger.error(f"Status check failed: {e}")
            return None
    
    def handle_events(
        self,
        topics: List[str],
        handler: Callable[[Dict[str, Any]], None],
        timeout: int = 1000
    ):
        """Handle CDC events."""
        try:
            # Subscribe to topics
            self.consumer.subscribe(topics)
            
            while True:
                # Poll for messages
                messages = self.consumer.poll(
                    timeout_ms=timeout
                )
                
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            # Process event
                            event = record.value
                            handler(event)
                            
                            # Commit offset
                            self.consumer.commit()
                            
                        except Exception as e:
                            logger.error(
                                f"Event handling failed: {e}"
                            )
                
        except Exception as e:
            logger.error(f"Event processing failed: {e}")
            raise
    
    def update_schema(
        self,
        table_name: str,
        changes: Dict[str, Any]
    ) -> bool:
        """Handle schema evolution."""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # Create ALTER TABLE statement
                    alter_stmt = f"ALTER TABLE {table_name}"
                    alter_parts = []
                    
                    for col_name, col_def in changes.items():
                        if 'add' in col_def:
                            # Add column
                            alter_parts.append(
                                f"ADD COLUMN {col_name} {col_def['add']}"
                            )
                        elif 'modify' in col_def:
                            # Modify column
                            alter_parts.append(
                                f"ALTER COLUMN {col_name} TYPE {col_def['modify']}"
                            )
                        elif 'drop' in col_def and col_def['drop']:
                            # Drop column
                            alter_parts.append(
                                f"DROP COLUMN {col_name}"
                            )
                    
                    if alter_parts:
                        # Execute schema change
                        cur.execute(
                            f"{alter_stmt} {', '.join(alter_parts)}"
                        )
                        conn.commit()
                        
                        # Restart connector to pick up changes
                        self._restart_connector()
                        return True
                    
                    return False
                    
        except Exception as e:
            logger.error(f"Schema update failed: {e}")
            return False
    
    def _restart_connector(self):
        """Restart connector to apply schema changes."""
        try:
            # Get connectors
            response = requests.get(
                f"{self.connect_url}/connectors"
            )
            
            if response.status_code == 200:
                connectors = response.json()
                
                # Restart each connector
                for name in connectors:
                    restart_response = requests.post(
                        f"{self.connect_url}/connectors/{name}/restart"
                    )
                    
                    if restart_response.status_code != 204:
                        logger.warning(
                            f"Failed to restart connector {name}"
                        )
            
        except Exception as e:
            logger.error(f"Connector restart failed: {e}")
    
    def close(self):
        """Close connections."""
        try:
            self.consumer.close()
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")

def main():
    """Run Debezium integration example."""
    # Configuration
    connect_url = "http://localhost:8083"
    
    kafka_config = {
        'bootstrap.servers': 'localhost:9092'
    }
    
    db_config = {
        'dbname': 'testdb',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost'
    }
    
    debezium = DebeziumIntegration(
        connect_url,
        kafka_config,
        db_config
    )
    
    try:
        # Create connector
        connector_config = {
            'database.hostname': 'localhost',
            'database.port': '5432',
            'database.user': 'postgres',
            'database.password': 'postgres',
            'database.dbname': 'testdb',
            'database.server.name': 'postgres',
            'table.include.list': 'public.users',
            'plugin.name': 'pgoutput'
        }
        
        debezium.create_connector(
            'postgres-connector',
            connector_config
        )
        
        # Handle events
        def event_handler(event: Dict[str, Any]):
            logger.info(f"Received event: {event}")
        
        debezium.handle_events(
            ['postgres.public.users'],
            event_handler
        )
        
    finally:
        debezium.close()

if __name__ == '__main__':
    main() 