"""
Data Processors Module
-----------------

Handles data processing:
1. Validation
2. Transformation
3. Aggregation
"""

import logging
from typing import Dict, Any
from datetime import datetime
import json
from prometheus_client import Counter, Histogram
from kafka import KafkaProducer, KafkaConsumer

from .models import SensorData
from .utils import retry_with_logging

logger = logging.getLogger(__name__)

# Metrics
MESSAGES_PROCESSED = Counter(
    'processor_messages_total',
    'Number of messages processed',
    ['status']
)
PROCESSING_TIME = Histogram(
    'processor_time_seconds',
    'Time spent processing messages'
)

class DataProcessor:
    """Data processor implementation."""
    
    def __init__(
        self,
        kafka_servers: str,
        input_topic: str,
        output_topic: str,
        group_id: str
    ):
        """Initialize processor."""
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode()
        )
        
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=kafka_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode())
        )
        
        self.output_topic = output_topic
        self.running = False
    
    def start(self):
        """Start processor."""
        try:
            self.running = True
            logger.info("Data processor started")
            
            for message in self.consumer:
                if not self.running:
                    break
                
                with PROCESSING_TIME.time():
                    try:
                        # Process message
                        data = self._process_message(
                            message.value
                        )
                        
                        # Send processed data
                        self.producer.send(
                            self.output_topic,
                            data
                        )
                        
                        MESSAGES_PROCESSED.labels(
                            status='success'
                        ).inc()
                        
                    except Exception as e:
                        logger.error(
                            f"Message processing failed: {e}"
                        )
                        MESSAGES_PROCESSED.labels(
                            status='error'
                        ).inc()
            
        except Exception as e:
            logger.error(f"Processor failed: {e}")
            raise
    
    def stop(self):
        """Stop processor."""
        try:
            self.running = False
            self.consumer.close()
            self.producer.close()
            logger.info("Data processor stopped")
            
        except Exception as e:
            logger.error(f"Processor stop failed: {e}")
    
    @retry_with_logging
    def _process_message(
        self,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process message with validation and transformation."""
        # Validate data
        if not all(
            k in data for k in
            ['sensor_id', 'metric', 'value', 'timestamp']
        ):
            raise ValueError("Invalid message format")
        
        # Transform data
        processed = {
            'sensor_id': data['sensor_id'],
            'metric': data['metric'],
            'value': float(data['value']),
            'timestamp': data['timestamp'],
            'processed_at': datetime.now().isoformat()
        }
        
        return processed 