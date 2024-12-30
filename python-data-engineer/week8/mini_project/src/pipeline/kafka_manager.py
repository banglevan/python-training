"""
Kafka management implementation.
"""

import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import json
from confluent_kafka import (
    Producer, Consumer, KafkaError,
    TopicPartition, admin
)
from confluent_kafka.admin import (
    AdminClient, NewTopic,
    ConfigResource, ConfigEntry,
    RESOURCE_TOPIC
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaManager:
    """Manages Kafka topics and message handling."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Kafka manager."""
        self.config = config
        self.admin_client = AdminClient({
            'bootstrap.servers': config['bootstrap_servers']
        })
        self.producer = Producer({
            'bootstrap.servers': config['bootstrap_servers'],
            'client.id': config.get('client_id', 'data-sync-producer'),
            'acks': 'all'
        })
        self._consumers: Dict[str, Consumer] = {}
    
    def create_topics(
        self,
        topics: List[Dict[str, Any]],
        timeout: float = 30.0
    ) -> Dict[str, bool]:
        """
        Create Kafka topics.
        
        Args:
            topics: List of topic configurations
            timeout: Operation timeout in seconds
        """
        try:
            new_topics = [
                NewTopic(
                    topic['name'],
                    num_partitions=topic.get('partitions', 1),
                    replication_factor=topic.get('replication', 1),
                    config=topic.get('config', {})
                )
                for topic in topics
            ]
            
            # Create topics
            fs = self.admin_client.create_topics(new_topics)
            
            # Wait for results
            results = {}
            for topic, f in fs.items():
                try:
                    f.result(timeout=timeout)
                    results[topic] = True
                except Exception as e:
                    logger.error(f"Failed to create topic {topic}: {e}")
                    results[topic] = False
            
            return results
            
        except Exception as e:
            logger.error(f"Topic creation failed: {e}")
            raise
    
    def setup_consumer(
        self,
        group_id: str,
        topics: List[str],
        config: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Setup consumer for topics."""
        try:
            consumer_config = {
                'bootstrap.servers': self.config['bootstrap_servers'],
                'group.id': group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            }
            
            if config:
                consumer_config.update(config)
            
            consumer = Consumer(consumer_config)
            consumer.subscribe(topics)
            
            self._consumers[group_id] = consumer
            return True
            
        except Exception as e:
            logger.error(f"Consumer setup failed: {e}")
            return False
    
    def produce_message(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[List[tuple]] = None,
        callback: Optional[Callable] = None
    ) -> bool:
        """Produce message to topic."""
        try:
            self.producer.produce(
                topic=topic,
                value=json.dumps(value).encode('utf-8'),
                key=key.encode('utf-8') if key else None,
                headers=headers,
                callback=callback or self._delivery_report
            )
            
            # Trigger any available delivery reports
            self.producer.poll(0)
            return True
            
        except Exception as e:
            logger.error(f"Message production failed: {e}")
            return False
    
    def consume_batch(
        self,
        group_id: str,
        batch_size: int = 100,
        timeout: float = 1.0
    ) -> List[Dict[str, Any]]:
        """Consume batch of messages."""
        try:
            consumer = self._consumers.get(group_id)
            if not consumer:
                raise ValueError(f"No consumer found for group: {group_id}")
            
            messages = []
            message_count = 0
            
            while message_count < batch_size:
                msg = consumer.poll(timeout=timeout)
                
                if msg is None:
                    break
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    break
                
                try:
                    messages.append({
                        'topic': msg.topic(),
                        'partition': msg.partition(),
                        'offset': msg.offset(),
                        'key': msg.key().decode('utf-8') if msg.key() else None,
                        'value': json.loads(msg.value().decode('utf-8')),
                        'timestamp': msg.timestamp()[1],
                        'headers': msg.headers()
                    })
                    message_count += 1
                    
                except Exception as e:
                    logger.error(f"Message processing failed: {e}")
            
            return messages
            
        except Exception as e:
            logger.error(f"Batch consumption failed: {e}")
            return []
    
    def commit_offsets(self, group_id: str) -> bool:
        """Commit consumer offsets."""
        try:
            consumer = self._consumers.get(group_id)
            if consumer:
                consumer.commit()
                return True
            return False
            
        except Exception as e:
            logger.error(f"Offset commit failed: {e}")
            return False
    
    def _delivery_report(self, err, msg):
        """Delivery report handler."""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} [{msg.partition()}]"
            )
    
    def close(self):
        """Close Kafka connections."""
        try:
            # Close consumers
            for consumer in self._consumers.values():
                consumer.close()
            
            # Close producer
            self.producer.flush()
            self.producer.close()
            
        except Exception as e:
            logger.error(f"Kafka cleanup failed: {e}") 