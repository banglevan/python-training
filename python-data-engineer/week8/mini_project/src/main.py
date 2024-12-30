"""
Main application entry point.
"""

import logging
import yaml
import time
from typing import Dict, Any
from pathlib import Path

from connectors import create_connector
from pipeline import (
    create_processor,
    KafkaManager,
    FlinkProcessor,
    CacheManager
)
from sync import create_sync_manager
from monitoring import create_monitoring, AlertSeverity

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataSyncApp:
    """Main data synchronization application."""
    
    def __init__(self, config_path: str):
        """Initialize application."""
        self.config = self._load_config(config_path)
        self.connectors = {}
        self.kafka = None
        self.flink = None
        self.cache = None
        self.sync_manager = None
        self.monitoring = None
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Config loading failed: {e}")
            raise
    
    def initialize(self) -> bool:
        """Initialize application components."""
        try:
            # Initialize monitoring first
            self.monitoring = create_monitoring(self.config['monitoring'])
            if not self.monitoring:
                raise RuntimeError("Monitoring initialization failed")
            
            # Initialize data connectors
            for name, conn_config in self.config['connectors'].items():
                connector = create_connector(
                    conn_config['type'],
                    conn_config
                )
                if connector:
                    self.connectors[name] = connector
                    self.monitoring.record_event(
                        'connector',
                        {
                            'name': name,
                            'type': conn_config['type'],
                            'status': 'initialized'
                        }
                    )
            
            # Initialize pipeline components
            self.kafka = KafkaManager(self.config['kafka'])
            self.flink = FlinkProcessor(self.config['flink'])
            self.cache = CacheManager(self.config['cache'])
            
            # Initialize sync manager
            self.sync_manager = create_sync_manager(self.config['sync'])
            if not self.sync_manager:
                raise RuntimeError("Sync manager initialization failed")
            
            # Register connectors with sync manager
            for name, connector in self.connectors.items():
                self.sync_manager.register_connector(name, connector)
            
            logger.info("Application initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Application initialization failed: {e}")
            return False
    
    def start(self):
        """Start the application."""
        try:
            logger.info("Starting data sync application")
            
            # Create Kafka topics
            topics = self.config['kafka']['topics']
            self.kafka.create_topics(topics)
            
            # Setup Flink tables
            for table in self.config['flink']['tables']:
                if table['type'] == 'source':
                    self.flink.create_source_table(
                        table['name'],
                        table['topic'],
                        table['schema']
                    )
                else:
                    self.flink.create_sink_table(
                        table['name'],
                        table['topic'],
                        table['schema']
                    )
            
            # Start sync processes
            for sync_config in self.config['sync']['processes']:
                self.sync_manager.start_sync(
                    sync_config['source'],
                    sync_config['targets'],
                    sync_config['entity_type']
                )
            
            # Main application loop
            while True:
                try:
                    self._check_health()
                    time.sleep(self.config.get('health_check_interval', 60))
                except KeyboardInterrupt:
                    logger.info("Shutting down...")
                    break
                
        except Exception as e:
            logger.error(f"Application execution failed: {e}")
            self.monitoring.record_event(
                'application',
                {'status': 'failed', 'error': str(e)},
                alert=True,
                alert_severity=AlertSeverity.CRITICAL
            )
        finally:
            self.cleanup()
    
    def _check_health(self):
        """Check health of all components."""
        try:
            # Check connector health
            for name, connector in self.connectors.items():
                health = connector.health_check()
                self.monitoring.record_event(
                    'health',
                    {
                        'name': name,
                        'type': 'connector',
                        'healthy': health['connected'],
                        'details': health
                    },
                    alert=not health['connected'],
                    alert_severity=AlertSeverity.ERROR
                )
            
            # Check Kafka health
            # Add Kafka health checks
            
            # Check Flink health
            # Add Flink health checks
            
            # Check cache health
            # Add cache health checks
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            self.monitoring.record_event(
                'health',
                {'status': 'failed', 'error': str(e)},
                alert=True,
                alert_severity=AlertSeverity.ERROR
            )
    
    def cleanup(self):
        """Cleanup application resources."""
        try:
            # Cleanup connectors
            for connector in self.connectors.values():
                connector.close()
            
            # Cleanup pipeline components
            if self.kafka:
                self.kafka.close()
            if self.flink:
                self.flink.stop()
            if self.cache:
                self.cache.close()
            
            # Cleanup sync manager
            if self.sync_manager:
                self.sync_manager.close()
            
            # Cleanup monitoring
            if self.monitoring:
                self.monitoring.close()
            
            logger.info("Application cleanup completed")
            
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")

def main():
    """Application entry point."""
    try:
        # Get config path
        config_path = Path(__file__).parent.parent / 'config' / 'config.yml'
        
        # Create and start application
        app = DataSyncApp(str(config_path))
        if app.initialize():
            app.start()
        else:
            logger.error("Application initialization failed")
            
    except Exception as e:
        logger.error(f"Application failed: {e}")

if __name__ == "__main__":
    main() 