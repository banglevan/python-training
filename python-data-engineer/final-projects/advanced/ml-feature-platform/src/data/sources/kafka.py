"""
Kafka data source for real-time feature extraction.
"""

from typing import Dict, Any, Optional, Callable
from kafka import KafkaConsumer
import json
from datetime import datetime
import logging
from src.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)

class KafkaSource:
    """Kafka source for real-time feature extraction."""
    
    def __init__(
        self,
        bootstrap_servers: list,
        topics: list,
        group_id: str
    ):
        """Initialize Kafka source."""
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.metrics = MetricsCollector()
    
    def process_events(
        self,
        handler: Callable[[Dict[str, Any]], None],
        timeout_ms: int = 1000
    ) -> None:
        """
        Process Kafka events with given handler.
        
        Args:
            handler: Callback function to process events
            timeout_ms: Consumer timeout in milliseconds
        """
        try:
            self.metrics.start_operation("process_events")
            
            for message in self.consumer:
                try:
                    # Process message
                    event = message.value
                    event['timestamp'] = datetime.fromtimestamp(
                        message.timestamp / 1000.0
                    )
                    
                    # Call handler
                    handler(event)
                    
                    # Record metrics
                    self.metrics.record_value(
                        "events_processed",
                        1,
                        {"topic": message.topic}
                    )
                    
                except Exception as e:
                    logger.error(f"Failed to process event: {e}")
                    self.metrics.record_error("process_event")
                    continue
                    
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")
            self.metrics.record_error("kafka_consumer")
            raise
        finally:
            self.consumer.close()
    
    def get_latest_events(
        self,
        num_events: int = 1000,
        timeout_ms: int = 1000
    ) -> list:
        """
        Get latest events from Kafka.
        
        Args:
            num_events: Number of events to fetch
            timeout_ms: Consumer timeout in milliseconds
            
        Returns:
            List of latest events
        """
        try:
            self.metrics.start_operation("get_latest_events")
            
            events = []
            message_count = 0
            
            while message_count < num_events:
                message_batch = self.consumer.poll(
                    timeout_ms=timeout_ms,
                    max_records=num_events - message_count
                )
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        event = message.value
                        event['timestamp'] = datetime.fromtimestamp(
                            message.timestamp / 1000.0
                        )
                        events.append(event)
                        message_count += 1
                
                if message_count >= num_events:
                    break
            
            duration = self.metrics.end_operation("get_latest_events")
            logger.info(
                f"Retrieved {len(events)} events in {duration:.2f}s"
            )
            
            return events
            
        except Exception as e:
            logger.error(f"Failed to get latest events: {e}")
            self.metrics.record_error("get_latest_events")
            raise
        finally:
            self.consumer.close() 