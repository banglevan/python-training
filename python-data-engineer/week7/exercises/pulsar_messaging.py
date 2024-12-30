"""
Pulsar Messaging Exercise
---------------------

Practice with:
1. Topics & subscriptions
2. Multi-tenancy
3. Geo-replication
"""

import pulsar
import json
import logging
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
import functools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PulsarMessaging:
    """Pulsar messaging handler."""
    
    def __init__(
        self,
        service_url: str = 'pulsar://localhost:6650',
        admin_url: str = 'http://localhost:8080',
        tenant: str = 'public',
        namespace: str = 'default'
    ):
        """Initialize Pulsar client."""
        self.client = pulsar.Client(service_url)
        self.admin_client = pulsar.admin.AdminClient(admin_url)
        self.tenant = tenant
        self.namespace = namespace
        
        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # Track producers and consumers
        self.producers = {}
        self.consumers = {}
    
    def create_tenant(
        self,
        tenant: str,
        allowed_clusters: List[str]
    ):
        """Create Pulsar tenant."""
        try:
            self.admin_client.tenants().create_tenant(
                tenant,
                {
                    'allowed_clusters': allowed_clusters,
                    'admin_roles': []
                }
            )
            logger.info(f"Created tenant: {tenant}")
            
        except Exception as e:
            logger.error(f"Tenant creation failed: {e}")
            raise
    
    def create_namespace(
        self,
        tenant: str,
        namespace: str,
        policies: Optional[Dict] = None
    ):
        """Create Pulsar namespace."""
        try:
            namespace_path = f"{tenant}/{namespace}"
            self.admin_client.namespaces().create_namespace(
                namespace_path
            )
            
            if policies:
                # Set retention policy
                if 'retention' in policies:
                    self.admin_client.namespaces().set_retention(
                        namespace_path,
                        policies['retention']
                    )
                
                # Set replication clusters
                if 'clusters' in policies:
                    self.admin_client.namespaces().set_clusters(
                        namespace_path,
                        policies['clusters']
                    )
            
            logger.info(
                f"Created namespace: {namespace_path}"
            )
            
        except Exception as e:
            logger.error(f"Namespace creation failed: {e}")
            raise
    
    def get_producer(
        self,
        topic: str,
        **config
    ) -> pulsar.Producer:
        """Get or create producer for topic."""
        try:
            if topic not in self.producers:
                # Create producer
                producer = self.client.create_producer(
                    f"persistent://{self.tenant}/"
                    f"{self.namespace}/{topic}",
                    **config
                )
                self.producers[topic] = producer
            
            return self.producers[topic]
            
        except Exception as e:
            logger.error(f"Producer creation failed: {e}")
            raise
    
    def get_consumer(
        self,
        topic: str,
        subscription_name: str,
        subscription_type: str = 'Shared',
        **config
    ) -> pulsar.Consumer:
        """Get or create consumer for topic."""
        try:
            consumer_key = f"{topic}_{subscription_name}"
            
            if consumer_key not in self.consumers:
                # Create consumer
                consumer = self.client.subscribe(
                    f"persistent://{self.tenant}/"
                    f"{self.namespace}/{topic}",
                    subscription_name=subscription_name,
                    subscription_type=getattr(
                        pulsar.ConsumerType,
                        subscription_type
                    ),
                    **config
                )
                self.consumers[consumer_key] = consumer
            
            return self.consumers[consumer_key]
            
        except Exception as e:
            logger.error(f"Consumer creation failed: {e}")
            raise
    
    def send_message(
        self,
        topic: str,
        message: Dict[str, Any],
        key: Optional[str] = None,
        properties: Optional[Dict] = None,
        **producer_config
    ):
        """Send message to topic."""
        try:
            producer = self.get_producer(
                topic,
                **producer_config
            )
            
            if key:
                producer.send(
                    json.dumps(message).encode(),
                    partition_key=key,
                    properties=properties or {}
                )
            else:
                producer.send(
                    json.dumps(message).encode(),
                    properties=properties or {}
                )
            
            logger.info(f"Sent message to topic: {topic}")
            
        except Exception as e:
            logger.error(f"Message send failed: {e}")
            raise
    
    def consume_messages(
        self,
        topic: str,
        subscription_name: str,
        callback: Callable,
        subscription_type: str = 'Shared',
        **consumer_config
    ):
        """Consume messages from topic."""
        try:
            consumer = self.get_consumer(
                topic,
                subscription_name,
                subscription_type,
                **consumer_config
            )
            
            logger.info(
                f"Started consuming from topic: {topic}"
            )
            
            while True:
                try:
                    # Receive message
                    msg = consumer.receive()
                    
                    try:
                        # Process message
                        message = json.loads(
                            msg.data().decode()
                        )
                        callback(
                            message,
                            msg.properties()
                        )
                        
                        # Acknowledge successful processing
                        consumer.acknowledge(msg)
                        
                    except Exception as e:
                        logger.error(
                            f"Message processing failed: {e}"
                        )
                        # Negative acknowledge failed message
                        consumer.negative_acknowledge(msg)
                    
                except KeyboardInterrupt:
                    break
                    
        except Exception as e:
            logger.error(f"Message consumption failed: {e}")
            raise
        finally:
            consumer.close()
    
    def setup_geo_replication(
        self,
        tenant: str,
        namespace: str,
        clusters: List[str]
    ):
        """Set up geo-replication for namespace."""
        try:
            namespace_path = f"{tenant}/{namespace}"
            
            # Set replication clusters
            self.admin_client.namespaces().set_clusters(
                namespace_path,
                clusters
            )
            
            # Set replication policy
            self.admin_client.namespaces().set_replication_policy(
                namespace_path,
                {
                    'replication_clusters': clusters,
                    'cleanup_delay_minutes': 10
                }
            )
            
            logger.info(
                f"Set up geo-replication for "
                f"{namespace_path} across {clusters}"
            )
            
        except Exception as e:
            logger.error(
                f"Geo-replication setup failed: {e}"
            )
            raise
    
    def close(self):
        """Clean up resources."""
        # Close producers
        for producer in self.producers.values():
            producer.close()
        
        # Close consumers
        for consumer in self.consumers.values():
            consumer.close()
        
        # Close client
        self.client.close()

def message_handler(
    message: Dict[str, Any],
    properties: Dict[str, str]
):
    """Example message handler."""
    logger.info(f"Received message: {message}")
    logger.info(f"Properties: {properties}")
    time.sleep(1)  # Simulate processing

def main():
    """Run Pulsar messaging example."""
    messaging = PulsarMessaging()
    
    try:
        # Set up multi-tenancy
        messaging.create_tenant(
            'example_tenant',
            ['standalone']
        )
        
        messaging.create_namespace(
            'example_tenant',
            'example_ns',
            {
                'retention': {
                    'size_in_mb': 100,
                    'time_in_minutes': 1440
                },
                'clusters': ['standalone']
            }
        )
        
        # Set up geo-replication
        messaging.setup_geo_replication(
            'example_tenant',
            'example_ns',
            ['standalone', 'secondary']
        )
        
        # Send some messages
        for i in range(10):
            messaging.send_message(
                'example_topic',
                {
                    'id': i,
                    'data': f"Message {i}",
                    'timestamp': datetime.now().isoformat()
                },
                key=str(i),
                properties={'source': 'example'}
            )
        
        # Consume messages
        messaging.consume_messages(
            'example_topic',
            'example_subscription',
            message_handler,
            subscription_type='Failover'
        )
        
    finally:
        messaging.close()

if __name__ == '__main__':
    main() 