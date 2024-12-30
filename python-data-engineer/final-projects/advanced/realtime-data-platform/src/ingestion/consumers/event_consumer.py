"""
Event consumer for e-commerce events.
"""

from typing import Dict, Any, Callable
from kafka import KafkaConsumer
import json
from datetime import datetime
from src.utils.config import Config
from src.utils.logging import get_logger
from src.utils.metrics import MetricsTracker
from src.storage.delta.writer import DeltaWriter
from src.storage.elasticsearch.writer import ElasticsearchWriter

logger = get_logger(__name__)

class EventConsumer:
    """Consumer for e-commerce events."""
    
    def __init__(
        self,
        config: Config,
        delta_writer: DeltaWriter,
        es_writer: ElasticsearchWriter
    ):
        """Initialize consumer."""
        self.config = config
        self.metrics = MetricsTracker()
        self.delta_writer = delta_writer
        self.es_writer = es_writer
        
        self.consumer = KafkaConsumer(
            self.config.kafka.topics['events']['name'],
            bootstrap_servers=config.kafka.bootstrap_servers,
            group_id=config.kafka.consumer_group,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda v: v.decode('utf-8')
        )
    
    def process_event(self, event: Dict[str, Any]) -> None:
        """Process single event."""
        try:
            self.metrics.start_operation('process_event')
            
            # Write to Delta Lake
            self.delta_writer.write(event)
            
            # Write to Elasticsearch
            self.es_writer.write(event)
            
            duration = self.metrics.end_operation('process_event')
            logger.debug(
                f"Processed event {event['event_id']} in {duration:.3f} seconds"
            )
            
            # Record event metrics
            self.metrics.record_value(
                'events_processed',
                1,
                {'event_type': event['event_type']}
            )
            
        except Exception as e:
            logger.error(f"Failed to process event: {e}")
            self.metrics.record_value('processing_errors', 1)
            raise
    
    def run(self) -> None:
        """Run consumer."""
        try:
            logger.info(
                f"Starting consumer for topic: {self.config.kafka.topics['events']['name']}"
            )
            
            for message in self.consumer:
                self.process_event(message.value)
                
                # Log metrics periodically
                if self.metrics.get_metrics()['events_processed']['count'] % 1000 == 0:
                    self._log_metrics()
                    
        except KeyboardInterrupt:
            logger.info("Stopping consumer")
            self.consumer.close()
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            self.consumer.close()
            raise
    
    def _log_metrics(self) -> None:
        """Log consumer metrics."""
        metrics = self.metrics.get_metrics()
        logger.info(
            f"Consumer metrics: "
            f"processed={metrics['events_processed']['count']}, "
            f"errors={metrics.get('processing_errors', {'count': 0})['count']}, "
            f"avg_processing_time={metrics['process_event']['total_time'] / metrics['process_event']['count']:.3f}s"
        ) 