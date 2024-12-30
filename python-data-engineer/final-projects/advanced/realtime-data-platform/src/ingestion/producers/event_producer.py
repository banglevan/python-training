"""
Event producer for e-commerce events.
"""

from dataclasses import dataclass
from datetime import datetime
import json
import random
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from src.utils.config import Config
from src.utils.logging import get_logger
from src.utils.metrics import MetricsTracker

logger = get_logger(__name__)

@dataclass
class Event:
    """E-commerce event."""
    event_id: str
    event_type: str
    user_id: int
    product_id: str
    quantity: int
    price: float
    timestamp: str
    session_id: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'event_id': self.event_id,
            'event_type': self.event_type,
            'user_id': self.user_id,
            'product_id': self.product_id,
            'quantity': self.quantity,
            'price': self.price,
            'timestamp': self.timestamp,
            'session_id': self.session_id
        }

class EventProducer:
    """Producer for e-commerce events."""
    
    def __init__(self, config: Config):
        """Initialize producer."""
        self.config = config
        self.metrics = MetricsTracker()
        self.producer = KafkaProducer(
            bootstrap_servers=config.kafka.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: str(v).encode('utf-8')
        )
    
    def generate_event(self) -> Event:
        """Generate random event."""
        event_types = ['view', 'cart', 'purchase']
        products = [f'product_{i}' for i in range(1, 101)]
        
        return Event(
            event_id=f"evt_{random.randint(1, 1000000)}",
            event_type=random.choice(event_types),
            user_id=random.randint(1, 1000),
            product_id=random.choice(products),
            quantity=random.randint(1, 5),
            price=round(random.uniform(10, 1000), 2),
            timestamp=datetime.now().isoformat(),
            session_id=f"session_{random.randint(1, 10000)}"
        )
    
    def send_event(self, event: Event) -> None:
        """Send event to Kafka."""
        try:
            self.metrics.start_operation('send_event')
            
            future = self.producer.send(
                self.config.kafka.topics['events']['name'],
                key=event.event_id,
                value=event.to_dict()
            )
            future.get(timeout=10)
            
            duration = self.metrics.end_operation('send_event')
            logger.debug(
                f"Sent event {event.event_id} in {duration:.3f} seconds"
            )
            
            # Record event metrics
            self.metrics.record_value(
                'events_produced',
                1,
                {'event_type': event.event_type}
            )
            
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
            raise

if __name__ == "__main__":
    config = Config(
        kafka=Config.KafkaConfig(
            bootstrap_servers=['localhost:9092'],
            topics={
                'events': Config.TopicConfig(
                    name='ecommerce_events',
                    partition=0,
                    replication_factor=1
                )
            }
        )
    )
    
    producer = EventProducer(config)
    producer.run() 