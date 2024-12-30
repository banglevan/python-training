"""
Data Collectors Module
------------------

Handles data collection from different sources:
1. MQTT for sensor data
2. System metrics
3. Application logs
"""

import json
import logging
from typing import Dict, Any, Optional, Callable
from datetime import datetime
import paho.mqtt.client as mqtt
from prometheus_client import Counter, Gauge

from .models import SensorData
from .utils import retry_with_logging

logger = logging.getLogger(__name__)

# Metrics
MESSAGES_RECEIVED = Counter(
    'collector_messages_received_total',
    'Number of messages received',
    ['source']
)
COLLECTOR_ERRORS = Counter(
    'collector_errors_total',
    'Number of collector errors',
    ['source']
)

class MQTTCollector:
    """MQTT data collector."""
    
    def __init__(
        self,
        broker: str,
        port: int,
        topics: list[str],
        callback: Callable
    ):
        """Initialize MQTT collector."""
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.broker = broker
        self.port = port
        self.topics = topics
        self.callback = callback
    
    def start(self):
        """Start MQTT collector."""
        try:
            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start()
            logger.info("MQTT collector started")
            
        except Exception as e:
            logger.error(f"MQTT collector start failed: {e}")
            raise
    
    def stop(self):
        """Stop MQTT collector."""
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("MQTT collector stopped")
            
        except Exception as e:
            logger.error(f"MQTT collector stop failed: {e}")
    
    def _on_connect(self, client, userdata, flags, rc):
        """Handle MQTT connection."""
        if rc == 0:
            logger.info("Connected to MQTT broker")
            for topic in self.topics:
                client.subscribe(topic)
        else:
            logger.error(f"MQTT connection failed: {rc}")
    
    def _on_message(self, client, userdata, msg):
        """Handle MQTT messages."""
        try:
            MESSAGES_RECEIVED.labels(source='mqtt').inc()
            
            payload = json.loads(msg.payload)
            sensor_id = msg.topic.split('/')[1]
            
            data = SensorData(
                sensor_id=sensor_id,
                metric=payload['metric'],
                value=payload['value'],
                timestamp=datetime.fromisoformat(
                    payload['timestamp']
                ),
                metadata=payload.get('metadata')
            )
            
            self.callback(data)
            
        except Exception as e:
            logger.error(f"MQTT message processing failed: {e}")
            COLLECTOR_ERRORS.labels(source='mqtt').inc() 