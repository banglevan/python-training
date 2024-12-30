"""
Real-time Data Pipeline Application
-----------------------------

Main application entry point.
"""

import os
import signal
import logging
from typing import Dict, Any
import threading
from prometheus_client import start_http_server
from dotenv import load_dotenv

from pipeline.collectors import MQTTCollector
from pipeline.processors import DataProcessor
from pipeline.storage import DataStorage
from pipeline.models import SensorData

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PipelineApp:
    """Pipeline application."""
    
    def __init__(self):
        """Initialize application components."""
        # Create components
        self.collector = MQTTCollector(
            broker=os.getenv('MQTT_BROKER'),
            port=int(os.getenv('MQTT_PORT')),
            topics=['sensors/+/data'],
            callback=self._handle_sensor_data
        )
        
        self.processor = DataProcessor(
            kafka_servers=os.getenv('KAFKA_SERVERS'),
            input_topic='sensor_data',
            output_topic='processed_data',
            group_id='processing_group'
        )
        
        self.storage = DataStorage(
            db_url=os.getenv('DATABASE_URL'),
            redis_host=os.getenv('REDIS_HOST'),
            redis_port=int(os.getenv('REDIS_PORT'))
        )
        
        # Control flag
        self.running = False
        
        # Processing thread
        self.processor_thread = None
    
    def start(self):
        """Start application."""
        try:
            logger.info("Starting pipeline application...")
            self.running = True
            
            # Start monitoring
            start_http_server(8080)
            
            # Start collector
            self.collector.start()
            
            # Start processor in thread
            self.processor_thread = threading.Thread(
                target=self.processor.start
            )
            self.processor_thread.start()
            
            # Register signal handlers
            signal.signal(
                signal.SIGTERM,
                self._signal_handler
            )
            signal.signal(
                signal.SIGINT,
                self._signal_handler
            )
            
            logger.info("Pipeline application started")
            
        except Exception as e:
            logger.error(f"Application startup failed: {e}")
            self.stop()
            raise
    
    def stop(self):
        """Stop application."""
        try:
            logger.info("Stopping pipeline application...")
            self.running = False
            
            # Stop components
            self.collector.stop()
            self.processor.stop()
            self.storage.close()
            
            # Wait for processor thread
            if self.processor_thread:
                self.processor_thread.join()
            
            logger.info("Pipeline application stopped")
            
        except Exception as e:
            logger.error(f"Application shutdown failed: {e}")
            raise
    
    def _handle_sensor_data(
        self,
        data: SensorData
    ):
        """Handle incoming sensor data."""
        try:
            # Store raw data
            self.storage.store_measurement({
                'sensor_id': data.sensor_id,
                'metric': data.metric,
                'value': data.value,
                'timestamp': data.timestamp.isoformat()
            })
            
            # Cache latest value
            self.storage.cache_data(
                f"sensor:{data.sensor_id}:{data.metric}",
                {
                    'value': data.value,
                    'timestamp': data.timestamp.isoformat()
                }
            )
            
        except Exception as e:
            logger.error(f"Data handling failed: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}")
        self.stop()

def main():
    """Application entry point."""
    app = PipelineApp()
    try:
        app.start()
        # Keep main thread alive
        while app.running:
            signal.pause()
    except KeyboardInterrupt:
        app.stop()

if __name__ == '__main__':
    main() 