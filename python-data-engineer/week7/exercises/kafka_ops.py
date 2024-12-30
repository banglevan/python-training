"""
Kafka Operations Exercise
---------------------

Practice with:
1. Topic management
2. Producer configurations
3. Consumer groups
4. Stream processing
"""

from kafka import (
    KafkaAdminClient,
    KafkaProducer,
    KafkaConsumer,
    TopicPartition
)
from kafka.admin import (
    NewTopic,
    ConfigResource,
    ConfigResourceType
)
from kafka.errors import KafkaError
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaOperator:
    """Kafka operations handler."""
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        client_id: str = 'kafka_operator'
    ):
        """Initialize Kafka connections."""
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        
        # Initialize admin client
        self.admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id
        )
        
        # Initialize producer with default config
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            key_serializer=str.encode,
            value_serializer=lambda v: json.dumps(v).encode(),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
            enable_idempotence=True
        )
        
        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    def create_topic(
        self,
        topic: str,
        num_partitions: int = 3,
        replication_factor: int = 1,
        config: Optional[Dict] = None
    ):
        """Create Kafka topic."""
        try:
            topic_configs = []
            if config:
                topic_configs = [
                    (key, str(value))
                    for key, value in config.items()
                ]
            
            new_topic = NewTopic(
                name=topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=dict(topic_configs)
            )
            
            self.admin.create_topics([new_topic])
            logger.info(f"Created topic: {topic}")
            
        except Exception as e:
            logger.error(f"Topic creation failed: {e}")
            raise
    
    def delete_topic(
        self,
        topic: str
    ):
        """Delete Kafka topic."""
        try:
            self.admin.delete_topics([topic])
            logger.info(f"Deleted topic: {topic}")
            
        except Exception as e:
            logger.error(f"Topic deletion failed: {e}")
            raise
    
    def get_topic_config(
        self,
        topic: str
    ) -> Dict[str, Any]:
        """Get topic configuration."""
        try:
            resource = ConfigResource(
                ConfigResourceType.TOPIC,
                topic
            )
            configs = self.admin.describe_configs(
                [resource]
            )
            return {
                entry.name: entry.value
                for entry in configs[0].entries
            }
            
        except Exception as e:
            logger.error(
                f"Failed to get topic config: {e}"
            )
            raise
    
    def produce_message(
        self,
        topic: str,
        message: Dict[str, Any],
        key: Optional[str] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None
    ):
        """Produce message to topic."""
        try:
            future = self.producer.send(
                topic,
                value=message,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms
            )
            
            # Wait for message to be sent
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Produced message to {topic} "
                f"[partition={record_metadata.partition}] "
                f"@ offset={record_metadata.offset}"
            )
            
        except Exception as e:
            logger.error(f"Message production failed: {e}")
            raise
    
    def create_consumer(
        self,
        group_id: str,
        auto_offset_reset: str = 'earliest',
        enable_auto_commit: bool = True,
        **kwargs
    ) -> KafkaConsumer:
        """Create configured consumer."""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=enable_auto_commit,
                key_deserializer=bytes.decode,
                value_deserializer=lambda v: json.loads(
                    v.decode()
                ),
                **kwargs
            )
            return consumer
            
        except Exception as e:
            logger.error(f"Consumer creation failed: {e}")
            raise
    
    def process_stream(
        self,
        input_topic: str,
        output_topic: str,
        processor_func,
        group_id: str,
        **consumer_config
    ):
        """Process message stream with transformation."""
        try:
            consumer = self.create_consumer(
                group_id,
                **consumer_config
            )
            consumer.subscribe([input_topic])
            
            logger.info(
                f"Started stream processing from "
                f"{input_topic} to {output_topic}"
            )
            
            for message in consumer:
                try:
                    # Process message
                    result = processor_func(
                        message.value
                    )
                    
                    # Produce result
                    if result is not None:
                        self.produce_message(
                            output_topic,
                            result,
                            key=message.key
                        )
                    
                    # Commit offset if auto-commit disabled
                    if not consumer.config['enable_auto_commit']:
                        consumer.commit()
                    
                except Exception as e:
                    logger.error(
                        f"Message processing failed: {e}"
                    )
                
        except KeyboardInterrupt:
            logger.info("Stopping stream processor")
        finally:
            consumer.close()
    
    def parallel_process_stream(
        self,
        input_topic: str,
        output_topic: str,
        processor_func,
        group_id: str,
        num_workers: int = 3,
        **consumer_config
    ):
        """Process stream with parallel workers."""
        try:
            # Create consumer group members
            futures = []
            for i in range(num_workers):
                worker_group = f"{group_id}_{i}"
                future = self.executor.submit(
                    self.process_stream,
                    input_topic,
                    output_topic,
                    processor_func,
                    worker_group,
                    **consumer_config
                )
                futures.append(future)
            
            logger.info(
                f"Started {num_workers} parallel processors"
            )
            
            # Wait for all workers
            for future in futures:
                future.result()
                
        except KeyboardInterrupt:
            logger.info("Stopping parallel processors")
        finally:
            self.executor.shutdown()
    
    def close(self):
        """Clean up resources."""
        self.producer.close()
        self.admin.close()
        self.executor.shutdown()

def example_processor(message: Dict[str, Any]) -> Dict[str, Any]:
    """Example message processor."""
    # Add processing timestamp
    message['processed_at'] = datetime.now().isoformat()
    
    # Add some transformation
    if 'value' in message:
        message['value'] = message['value'] * 2
    
    return message

def main():
    """Run Kafka operations example."""
    operator = KafkaOperator()
    
    try:
        # Create topics
        operator.create_topic(
            'input_topic',
            num_partitions=3,
            config={
                'retention.ms': 86400000,  # 1 day
                'cleanup.policy': 'delete'
            }
        )
        
        operator.create_topic(
            'output_topic',
            num_partitions=3
        )
        
        # Produce some messages
        for i in range(10):
            operator.produce_message(
                'input_topic',
                {
                    'id': i,
                    'value': i,
                    'timestamp': datetime.now().isoformat()
                },
                key=str(i)
            )
        
        # Start stream processing
        operator.parallel_process_stream(
            'input_topic',
            'output_topic',
            example_processor,
            'stream_processor_group',
            num_workers=3,
            enable_auto_commit=False
        )
        
    finally:
        operator.close()

if __name__ == '__main__':
    main() 