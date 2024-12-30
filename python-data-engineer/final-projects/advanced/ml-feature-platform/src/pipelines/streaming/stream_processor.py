"""
Real-time feature stream processor.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import json
import logging
from src.data.sources.kafka import KafkaSource
from src.features.transformations.customer_transforms import CustomerFeatureTransformer
from src.features.transformations.product_transforms import ProductFeatureTransformer
from src.features.validation.validators import FeatureValidator
from src.data.sinks.feast import FeastSink
from src.data.sinks.redis import RedisSink
from src.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)

class StreamProcessor:
    """Process streaming events for real-time features."""
    
    def __init__(
        self,
        kafka_config: Dict[str, Any],
        feast_config: Dict[str, Any],
        redis_config: Dict[str, Any]
    ):
        """Initialize processor."""
        self.kafka_source = KafkaSource(**kafka_config)
        self.feast_sink = FeastSink(**feast_config)
        self.redis_sink = RedisSink(**redis_config)
        
        self.customer_transformer = CustomerFeatureTransformer()
        self.product_transformer = ProductFeatureTransformer()
        self.validator = FeatureValidator()
        self.metrics = MetricsCollector()
    
    def process_event(self, event: Dict[str, Any]) -> None:
        """
        Process single event and update features.
        
        Args:
            event: Event dictionary
        """
        try:
            self.metrics.start_operation("process_event")
            
            # Extract event type and data
            event_type = event.get('type')
            
            if event_type == 'order':
                self._process_order_event(event)
            elif event_type == 'product_view':
                self._process_product_view_event(event)
            else:
                logger.warning(f"Unknown event type: {event_type}")
            
            duration = self.metrics.end_operation("process_event")
            logger.debug(f"Processed event in {duration:.3f}s")
            
        except Exception as e:
            logger.error(f"Failed to process event: {e}")
            self.metrics.record_error("process_event")
            raise
    
    def _process_order_event(self, event: Dict[str, Any]) -> None:
        """Process order event."""
        try:
            # Extract customer features
            customer_features = self.customer_transformer.compute_order_features(
                pd.DataFrame([event]),
                event['customer_id']
            )
            
            # Validate features
            validation_result = self.validator.validate_features(
                pd.DataFrame([customer_features])
            )
            
            if validation_result['passed']:
                # Store in Redis for immediate access
                self.redis_sink.store_features(
                    f"customer_{event['customer_id']}",
                    customer_features
                )
                
                # Push to Feast
                self.feast_sink.push_features_to_online_store(
                    "customer_features",
                    pd.DataFrame([customer_features])
                )
            else:
                logger.warning(
                    f"Feature validation failed for customer {event['customer_id']}"
                )
            
        except Exception as e:
            logger.error(f"Failed to process order event: {e}")
            raise
    
    def _process_product_view_event(self, event: Dict[str, Any]) -> None:
        """Process product view event."""
        try:
            # Extract product features
            product_features = self.product_transformer.compute_sales_features(
                pd.DataFrame([event]),
                event['product_id']
            )
            
            # Validate features
            validation_result = self.validator.validate_features(
                pd.DataFrame([product_features])
            )
            
            if validation_result['passed']:
                # Store in Redis for immediate access
                self.redis_sink.store_features(
                    f"product_{event['product_id']}",
                    product_features
                )
                
                # Push to Feast
                self.feast_sink.push_features_to_online_store(
                    "product_features",
                    pd.DataFrame([product_features])
                )
            else:
                logger.warning(
                    f"Feature validation failed for product {event['product_id']}"
                )
            
        except Exception as e:
            logger.error(f"Failed to process product view event: {e}")
            raise
    
    def run(self) -> None:
        """Run stream processor."""
        try:
            logger.info("Starting stream processor")
            
            def event_handler(event: Dict[str, Any]) -> None:
                self.process_event(event)
            
            # Start processing events
            self.kafka_source.process_events(
                handler=event_handler
            )
            
        except KeyboardInterrupt:
            logger.info("Stopping stream processor")
        except Exception as e:
            logger.error(f"Stream processor failed: {e}")
            raise 