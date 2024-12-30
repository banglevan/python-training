"""
Kafka CDC Implementation
------------------

Practice with:
1. Connect setup
2. Transform configs
3. Sink management
"""

import logging
from typing import Dict, Any, List, Optional
import json
import time
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaCDC:
    """Kafka CDC implementation."""
    
    def __init__(
        self,
        bootstrap_servers: str,
        connect_url: str,
        schema_registry_url: Optional[str] = None
    ):
        """Initialize Kafka CDC."""
        self.connect_url = connect_url
        
        # Kafka admin client
        self.admin = AdminClient({
            'bootstrap.servers': bootstrap_servers
        })
        
        # Kafka producer
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all'
        })
        
        # Schema registry client if provided
        self.schema_registry_url = schema_registry_url
    
    def setup_source_connector(
        self,
        name: str,
        config: Dict[str, Any]
    ) -> bool:
        """Setup source connector."""
        try:
            # Add common configs
            connector_config = {
                'name': name,
                'connector.class': config.get(
                    'connector.class',
                    'io.debezium.connector.postgresql.PostgresConnector'
                ),
                'tasks.max': config.get('tasks.max', '1'),
                'key.converter': 'org.apache.kafka.connect.json.JsonConverter',
                'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
                'key.converter.schemas.enable': 'true',
                'value.converter.schemas.enable': 'true',
                **config
            }
            
            # Create connector
            response = requests.post(
                f"{self.connect_url}/connectors",
                headers={'Content-Type': 'application/json'},
                data=json.dumps(connector_config)
            )
            
            if response.status_code in (200, 201):
                logger.info(f"Created source connector: {name}")
                return True
            else:
                logger.error(
                    f"Failed to create connector: {response.text}"
                )
                return False
                
        except Exception as e:
            logger.error(f"Connector setup failed: {e}")
            return False
    
    def setup_sink_connector(
        self,
        name: str,
        topics: List[str],
        sink_config: Dict[str, Any]
    ) -> bool:
        """Setup sink connector."""
        try:
            # Add common configs
            connector_config = {
                'name': name,
                'connector.class': sink_config.get(
                    'connector.class',
                    'io.confluent.connect.jdbc.JdbcSinkConnector'
                ),
                'tasks.max': sink_config.get('tasks.max', '1'),
                'topics': ','.join(topics),
                'key.converter': 'org.apache.kafka.connect.json.JsonConverter',
                'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
                **sink_config
            }
            
            # Create connector
            response = requests.post(
                f"{self.connect_url}/connectors",
                headers={'Content-Type': 'application/json'},
                data=json.dumps(connector_config)
            )
            
            if response.status_code in (200, 201):
                logger.info(f"Created sink connector: {name}")
                return True
            else:
                logger.error(
                    f"Failed to create sink: {response.text}"
                )
                return False
                
        except Exception as e:
            logger.error(f"Sink setup failed: {e}")
            return False
    
    def setup_transforms(
        self,
        connector_name: str,
        transforms: List[Dict[str, Any]]
    ) -> bool:
        """Setup SMTs (Single Message Transforms)."""
        try:
            # Get current config
            response = requests.get(
                f"{self.connect_url}/connectors/{connector_name}/config"
            )
            
            if response.status_code != 200:
                logger.error("Failed to get connector config")
                return False
            
            config = response.json()
            
            # Add transforms
            for i, transform in enumerate(transforms, 1):
                transform_name = f"transform{i}"
                for key, value in transform.items():
                    config[f"transforms.{transform_name}.{key}"] = value
            
            config["transforms"] = ",".join(
                f"transform{i}"
                for i in range(1, len(transforms) + 1)
            )
            
            # Update config
            response = requests.put(
                f"{self.connect_url}/connectors/{connector_name}/config",
                headers={'Content-Type': 'application/json'},
                data=json.dumps(config)
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Transform setup failed: {e}")
            return False
    
    def monitor_connector(
        self,
        name: str,
        interval: int = 30
    ):
        """Monitor connector status."""
        try:
            while True:
                response = requests.get(
                    f"{self.connect_url}/connectors/{name}/status"
                )
                
                if response.status_code == 200:
                    status = response.json()
                    connector_status = status['connector']['state']
                    task_states = [
                        task['state']
                        for task in status['tasks']
                    ]
                    
                    logger.info(
                        f"Connector: {connector_status}, "
                        f"Tasks: {task_states}"
                    )
                    
                    if connector_status == 'FAILED':
                        logger.error(
                            f"Connector failed: {status['connector']['trace']}"
                        )
                        break
                        
                time.sleep(interval)
                
        except Exception as e:
            logger.error(f"Monitoring failed: {e}")
    
    def delete_connector(self, name: str) -> bool:
        """Delete a connector."""
        try:
            response = requests.delete(
                f"{self.connect_url}/connectors/{name}"
            )
            return response.status_code == 204
            
        except Exception as e:
            logger.error(f"Connector deletion failed: {e}")
            return False
    
    def close(self):
        """Close connections."""
        try:
            self.producer.flush()
            self.producer.close()
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")

def main():
    """Run Kafka CDC example."""
    cdc = KafkaCDC(
        bootstrap_servers='localhost:9092',
        connect_url='http://localhost:8083'
    )
    
    try:
        # Setup source connector
        source_config = {
            'database.hostname': 'localhost',
            'database.port': '5432',
            'database.user': 'postgres',
            'database.password': 'postgres',
            'database.dbname': 'sourcedb',
            'database.server.name': 'dbserver1',
            'table.include.list': 'public.users'
        }
        
        cdc.setup_source_connector(
            'postgres-source',
            source_config
        )
        
        # Setup transforms
        transforms = [
            {
                'type': 'org.apache.kafka.connect.transforms.ExtractField$Key',
                'field': 'id'
            },
            {
                'type': 'org.apache.kafka.connect.transforms.ValueToKey',
                'fields': 'id'
            }
        ]
        
        cdc.setup_transforms('postgres-source', transforms)
        
        # Setup sink connector
        sink_config = {
            'connection.url': 'jdbc:postgresql://localhost:5432/targetdb',
            'connection.user': 'postgres',
            'connection.password': 'postgres',
            'auto.create': 'true',
            'insert.mode': 'upsert',
            'pk.mode': 'record_key',
            'pk.fields': 'id'
        }
        
        cdc.setup_sink_connector(
            'postgres-sink',
            ['dbserver1.public.users'],
            sink_config
        )
        
        # Monitor connectors
        cdc.monitor_connector('postgres-source')
        
    finally:
        cdc.close()

if __name__ == '__main__':
    main() 