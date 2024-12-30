"""
IoT Integration Exercise
-------------------

Practice with:
1. Device management
2. Message routing
3. Security implementation
"""

import paho.mqtt.client as mqtt
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import time
import threading
import ssl
import hmac
import hashlib
import base64
import uuid
from dataclasses import dataclass, asdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Device:
    """IoT device representation."""
    device_id: str
    type: str
    location: str
    capabilities: List[str]
    auth_key: str
    last_seen: Optional[str] = None
    status: str = 'offline'
    metadata: Dict[str, Any] = None

class IoTIntegration:
    """IoT integration handler."""
    
    def __init__(
        self,
        broker_host: str = 'localhost',
        broker_port: int = 8883,
        ca_certs: Optional[str] = None
    ):
        """Initialize IoT integration."""
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.ca_certs = ca_certs
        
        # Device registry
        self.devices: Dict[str, Device] = {}
        self.device_lock = threading.Lock()
        
        # Message routing rules
        self.routing_rules: Dict[str, List[str]] = {}
        
        # Initialize MQTT client
        self.client = mqtt.Client(
            client_id=f'iot_gateway_{uuid.uuid4().hex}',
            clean_session=True
        )
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        
        # Configure TLS
        if ca_certs:
            self.client.tls_set(
                ca_certs=ca_certs,
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS,
                ciphers=None
            )
    
    def start(self):
        """Start IoT integration."""
        try:
            # Connect to broker
            self.client.connect(
                self.broker_host,
                self.broker_port,
                keepalive=60
            )
            
            # Start network loop
            self.client.loop_start()
            logger.info("IoT integration started")
            
        except Exception as e:
            logger.error(f"Start failed: {e}")
            raise
    
    def stop(self):
        """Stop IoT integration."""
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("IoT integration stopped")
            
        except Exception as e:
            logger.error(f"Stop failed: {e}")
            raise
    
    def register_device(
        self,
        device: Device
    ):
        """Register IoT device."""
        try:
            with self.device_lock:
                self.devices[device.device_id] = device
            
            # Subscribe to device topics
            self.client.subscribe([
                (f'devices/{device.device_id}/status', 1),
                (f'devices/{device.device_id}/telemetry', 1),
                (f'devices/{device.device_id}/events', 1)
            ])
            
            logger.info(
                f"Registered device: {device.device_id}"
            )
            
        except Exception as e:
            logger.error(f"Device registration failed: {e}")
            raise
    
    def remove_device(
        self,
        device_id: str
    ):
        """Remove IoT device."""
        try:
            with self.device_lock:
                self.devices.pop(device_id, None)
            
            # Unsubscribe from device topics
            self.client.unsubscribe([
                f'devices/{device_id}/status',
                f'devices/{device_id}/telemetry',
                f'devices/{device_id}/events'
            ])
            
            logger.info(f"Removed device: {device_id}")
            
        except Exception as e:
            logger.error(f"Device removal failed: {e}")
            raise
    
    def add_routing_rule(
        self,
        source_topic: str,
        destination_topics: List[str]
    ):
        """Add message routing rule."""
        try:
            self.routing_rules[source_topic] = destination_topics
            logger.info(
                f"Added routing rule: {source_topic} -> "
                f"{destination_topics}"
            )
            
        except Exception as e:
            logger.error(f"Rule addition failed: {e}")
            raise
    
    def remove_routing_rule(
        self,
        source_topic: str
    ):
        """Remove message routing rule."""
        try:
            self.routing_rules.pop(source_topic, None)
            logger.info(
                f"Removed routing rule for: {source_topic}"
            )
            
        except Exception as e:
            logger.error(f"Rule removal failed: {e}")
            raise
    
    def verify_message(
        self,
        device_id: str,
        message: Dict[str, Any],
        signature: str
    ) -> bool:
        """Verify message authenticity."""
        try:
            device = self.devices.get(device_id)
            if not device:
                return False
            
            # Create message signature
            message_str = json.dumps(
                message,
                sort_keys=True
            )
            expected_signature = base64.b64encode(
                hmac.new(
                    device.auth_key.encode(),
                    message_str.encode(),
                    hashlib.sha256
                ).digest()
            ).decode()
            
            return hmac.compare_digest(
                signature,
                expected_signature
            )
            
        except Exception as e:
            logger.error(f"Signature verification failed: {e}")
            return False
    
    def _on_connect(
        self,
        client,
        userdata,
        flags,
        rc
    ):
        """Handle connection established."""
        if rc == 0:
            logger.info("Connected to MQTT broker")
            
            # Resubscribe to device topics
            for device_id in self.devices:
                client.subscribe([
                    (f'devices/{device_id}/status', 1),
                    (f'devices/{device_id}/telemetry', 1),
                    (f'devices/{device_id}/events', 1)
                ])
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
            
            # Extract device ID from topic
            parts = message.topic.split('/')
            if len(parts) >= 2:
                device_id = parts[1]
                
                # Verify message if signature present
                if 'signature' in payload:
                    signature = payload.pop('signature')
                    if not self.verify_message(
                        device_id,
                        payload,
                        signature
                    ):
                        logger.warning(
                            f"Invalid message signature "
                            f"from device: {device_id}"
                        )
                        return
                
                # Update device status
                with self.device_lock:
                    if device_id in self.devices:
                        device = self.devices[device_id]
                        device.last_seen = datetime.now().isoformat()
                        
                        if 'status' in payload:
                            device.status = payload['status']
                
                # Apply routing rules
                self._route_message(
                    message.topic,
                    payload
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
    
    def _route_message(
        self,
        source_topic: str,
        message: Dict[str, Any]
    ):
        """Route message according to rules."""
        try:
            # Find matching rules
            for pattern, destinations in self.routing_rules.items():
                if mqtt.topic_matches_sub(pattern, source_topic):
                    # Forward message to destinations
                    for dest in destinations:
                        self.client.publish(
                            dest,
                            json.dumps(message),
                            qos=1
                        )
                    logger.debug(
                        f"Routed message from {source_topic} "
                        f"to {destinations}"
                    )
                    
        except Exception as e:
            logger.error(f"Message routing failed: {e}")

def main():
    """Run IoT integration example."""
    integration = IoTIntegration(
        broker_host='localhost',
        broker_port=8883,
        ca_certs='path/to/ca.crt'
    )
    
    try:
        # Start integration
        integration.start()
        
        # Register devices
        for i in range(3):
            device = Device(
                device_id=f'device_{i}',
                type='sensor',
                location=f'room_{i}',
                capabilities=['temperature', 'humidity'],
                auth_key=str(uuid.uuid4()),
                metadata={
                    'manufacturer': 'Example Inc',
                    'model': 'Sensor-2000'
                }
            )
            integration.register_device(device)
        
        # Add routing rules
        integration.add_routing_rule(
            'devices/+/temperature',
            ['analytics/temperature', 'storage/raw']
        )
        integration.add_routing_rule(
            'devices/+/events',
            ['alerts/events']
        )
        
        # Keep running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        pass
    finally:
        integration.stop()

if __name__ == '__main__':
    main() 