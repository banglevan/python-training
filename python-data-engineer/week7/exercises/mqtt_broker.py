"""
MQTT Broker Exercise
----------------

Practice with:
1. Topic structure
2. QoS levels
3. Retained messages
"""

import paho.mqtt.client as mqtt
import json
import logging
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
import time
from contextlib import contextmanager
import threading
import ssl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MQTTBroker:
    """MQTT broker operations handler."""
    
    def __init__(
        self,
        client_id: str,
        host: str = 'localhost',
        port: int = 1883,
        keepalive: int = 60,
        use_tls: bool = False,
        username: Optional[str] = None,
        password: Optional[str] = None
    ):
        """Initialize MQTT client."""
        self.client = mqtt.Client(
            client_id=client_id,
            clean_session=True
        )
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        self.client.on_subscribe = self._on_subscribe
        
        # Connection settings
        self.host = host
        self.port = port
        self.keepalive = keepalive
        
        # Security settings
        if use_tls:
            self.client.tls_set(
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS,
                ciphers=None
            )
        
        if username and password:
            self.client.username_pw_set(
                username,
                password
            )
        
        # Track subscriptions and callbacks
        self.topic_callbacks: Dict[str, Callable] = {}
        self.retained_messages: Dict[str, Dict] = {}
        
        # Track message IDs
        self.message_ids: Dict[int, bool] = {}
        self._message_id_lock = threading.Lock()
    
    def connect(self):
        """Connect to MQTT broker."""
        try:
            self.client.connect(
                self.host,
                self.port,
                self.keepalive
            )
            
            # Start network loop
            self.client.loop_start()
            logger.info("Connected to MQTT broker")
            
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise
    
    def disconnect(self):
        """Disconnect from MQTT broker."""
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("Disconnected from MQTT broker")
            
        except Exception as e:
            logger.error(f"Disconnect failed: {e}")
            raise
    
    def publish(
        self,
        topic: str,
        message: Dict[str, Any],
        qos: int = 0,
        retain: bool = False
    ) -> int:
        """Publish message to topic."""
        try:
            payload = json.dumps(message)
            message_info = self.client.publish(
                topic,
                payload,
                qos=qos,
                retain=retain
            )
            
            # Track message ID
            if message_info.mid:
                with self._message_id_lock:
                    self.message_ids[message_info.mid] = False
            
            logger.info(
                f"Published message to {topic} "
                f"[QoS: {qos}, Retain: {retain}]"
            )
            return message_info.mid
            
        except Exception as e:
            logger.error(f"Publish failed: {e}")
            raise
    
    def subscribe(
        self,
        topic: str,
        callback: Callable,
        qos: int = 0
    ):
        """Subscribe to topic."""
        try:
            # Register callback
            self.topic_callbacks[topic] = callback
            
            # Subscribe to topic
            result, mid = self.client.subscribe(
                topic,
                qos=qos
            )
            
            if result != mqtt.MQTT_ERR_SUCCESS:
                raise Exception(
                    f"Subscribe failed with code: {result}"
                )
            
            logger.info(
                f"Subscribed to {topic} [QoS: {qos}]"
            )
            
        except Exception as e:
            logger.error(f"Subscribe failed: {e}")
            raise
    
    def unsubscribe(
        self,
        topic: str
    ):
        """Unsubscribe from topic."""
        try:
            # Remove callback
            self.topic_callbacks.pop(topic, None)
            
            # Unsubscribe from topic
            result, mid = self.client.unsubscribe(topic)
            
            if result != mqtt.MQTT_ERR_SUCCESS:
                raise Exception(
                    f"Unsubscribe failed with code: {result}"
                )
            
            logger.info(f"Unsubscribed from {topic}")
            
        except Exception as e:
            logger.error(f"Unsubscribe failed: {e}")
            raise
    
    def _on_connect(
        self,
        client,
        userdata,
        flags,
        rc
    ):
        """Handle connection established."""
        if rc == 0:
            logger.info("Connected successfully")
            
            # Resubscribe to topics
            for topic in self.topic_callbacks:
                self.client.subscribe(topic)
        else:
            logger.error(
                f"Connection failed with code: {rc}"
            )
    
    def _on_message(
        self,
        client,
        userdata,
        message
    ):
        """Handle received message."""
        try:
            # Parse message
            payload = json.loads(message.payload)
            
            # Store retained message
            if message.retain:
                self.retained_messages[message.topic] = payload
            
            # Find matching callbacks
            for topic, callback in self.topic_callbacks.items():
                if mqtt.topic_matches_sub(topic, message.topic):
                    callback(
                        message.topic,
                        payload,
                        message.qos,
                        message.retain
                    )
            
        except Exception as e:
            logger.error(f"Message handling failed: {e}")
    
    def _on_disconnect(
        self,
        client,
        userdata,
        rc
    ):
        """Handle disconnection."""
        if rc != 0:
            logger.warning(
                f"Unexpected disconnection: {rc}"
            )
    
    def _on_publish(
        self,
        client,
        userdata,
        mid
    ):
        """Handle message published."""
        with self._message_id_lock:
            if mid in self.message_ids:
                self.message_ids[mid] = True
    
    def _on_subscribe(
        self,
        client,
        userdata,
        mid,
        granted_qos
    ):
        """Handle subscription complete."""
        logger.info(
            f"Subscription confirmed [QoS: {granted_qos}]"
        )
    
    def wait_for_publish(
        self,
        mid: int,
        timeout: float = 5.0
    ) -> bool:
        """Wait for publish confirmation."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self._message_id_lock:
                if self.message_ids.get(mid, False):
                    return True
            time.sleep(0.1)
        return False

def message_handler(
    topic: str,
    message: Dict[str, Any],
    qos: int,
    retain: bool
):
    """Example message handler."""
    logger.info(
        f"Received message from {topic} "
        f"[QoS: {qos}, Retain: {retain}]:"
    )
    logger.info(f"Payload: {message}")

def main():
    """Run MQTT broker example."""
    broker = MQTTBroker(
        client_id='mqtt_example',
        use_tls=True
    )
    
    try:
        # Connect to broker
        broker.connect()
        
        # Subscribe to topics
        broker.subscribe(
            'sensors/+/temperature',
            message_handler,
            qos=1
        )
        broker.subscribe(
            'sensors/+/humidity',
            message_handler,
            qos=2
        )
        
        # Publish messages
        for i in range(3):
            # Temperature reading
            mid = broker.publish(
                f'sensors/device_{i}/temperature',
                {
                    'value': 20 + i,
                    'unit': 'C',
                    'timestamp': datetime.now().isoformat()
                },
                qos=1
            )
            broker.wait_for_publish(mid)
            
            # Humidity reading (retained)
            mid = broker.publish(
                f'sensors/device_{i}/humidity',
                {
                    'value': 50 + i,
                    'unit': '%',
                    'timestamp': datetime.now().isoformat()
                },
                qos=2,
                retain=True
            )
            broker.wait_for_publish(mid)
        
        # Keep running
        time.sleep(30)
        
    finally:
        broker.disconnect()

if __name__ == '__main__':
    main() 