"""
Metrics collector for monitoring system performance.
"""

import psutil
import logging
from datetime import datetime
import time
from typing import Dict, Any
from elasticsearch import Elasticsearch
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MetricsCollector:
    """System metrics collector."""
    
    def __init__(self, es_host: str = "http://elasticsearch:9200"):
        """Initialize collector."""
        self.es = Elasticsearch(es_host)
        self.index = "system_metrics"
    
    def collect_system_metrics(self) -> Dict[str, Any]:
        """Collect system metrics."""
        return {
            "timestamp": datetime.now().isoformat(),
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_usage_percent": psutil.disk_usage('/').percent,
            "network_io": dict(psutil.net_io_counters()._asdict())
        }
    
    def collect_kafka_metrics(self) -> Dict[str, Any]:
        """Collect Kafka metrics."""
        # Add your Kafka metrics collection logic here
        return {}
    
    def collect_spark_metrics(self) -> Dict[str, Any]:
        """Collect Spark metrics."""
        # Add your Spark metrics collection logic here
        return {}
    
    def store_metrics(self, metrics: Dict[str, Any]) -> None:
        """Store metrics in Elasticsearch."""
        try:
            self.es.index(
                index=self.index,
                document=metrics
            )
        except Exception as e:
            logger.error(f"Failed to store metrics: {e}")
    
    def run(self, interval: int = 60):
        """Run metrics collection."""
        try:
            while True:
                # Collect metrics
                metrics = {
                    **self.collect_system_metrics(),
                    **self.collect_kafka_metrics(),
                    **self.collect_spark_metrics()
                }
                
                # Store metrics
                self.store_metrics(metrics)
                logger.debug(f"Stored metrics: {json.dumps(metrics, indent=2)}")
                
                # Wait for next collection
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Stopping metrics collection")
        except Exception as e:
            logger.error(f"Metrics collection failed: {e}")
            raise

if __name__ == "__main__":
    collector = MetricsCollector()
    collector.run() 