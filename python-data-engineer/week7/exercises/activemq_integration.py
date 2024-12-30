"""
ActiveMQ Integration Exercise
-------------------------

Practice with:
1. Topics & queues
2. Message persistence
3. Client integration
"""

import stomp
import json
import logging
from typing import Dict, Any, Optional, Callable
from datetime import datetime
import time
from contextlib import contextmanager
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ActiveMQClient:
    """ActiveMQ client integration."""
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 61613,
        username: str = 'admin',
        password: str = 'admin'
    ):
        """Initialize ActiveMQ connection."""
        self.conn = stomp.Connection(
            [(host, port)],
            auto_content_length=False
        )
        self.username = username
        self.password = password
        self.subscriptions = {}
    
    def connect(self):
        """Establish connection to ActiveMQ."""
        try:
            self.conn.connect(
                self.username,
                self.password,
                wait=True
            )
            logger.info("Connected to ActiveMQ")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close ActiveMQ connection."""
        if self.conn.is_connected():
            self.conn.disconnect()
            logger.info("Disconnected from ActiveMQ")
    
    def send_to_queue(
        self,
        queue: str,
        message: Dict[str, Any],
        persistent: bool = True,
        headers: Optional[Dict] = None
    ):
        """Send message to queue."""
        try:
            headers = headers or {}
            if persistent:
                headers['persistent'] = 'true'
            
            self.conn.send(
                destination=f'/queue/{queue}',
                body=json.dumps(message),
                headers=headers
            )
            logger.info(f"Sent message to queue: {queue}")
            
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            raise
    
    def send_to_topic(
        self,
        topic: str,
        message: Dict[str, Any],
        headers: Optional[Dict] = None
    ):
        """Publish message to topic."""
        try:
            self.conn.send(
                destination=f'/topic/{topic}',
                body=json.dumps(message),
                headers=headers or {}
            )
            logger.info(f"Published message to topic: {topic}")
            
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            raise
    
    def subscribe_to_queue(
        self,
        queue: str,
        callback: Callable,
        client_id: Optional[str] = None,
        durable: bool = False
    ):
        """Subscribe to queue."""
        try:
            subscription_id = str(uuid.uuid4())
            
            if durable and client_id:
                self.conn.set_client_id(client_id)
            
            self.conn.set_listener(
                subscription_id,
                QueueListener(callback)
            )
            
            self.conn.subscribe(
                destination=f'/queue/{queue}',
                id=subscription_id,
                ack='client-individual'
            )
            
            self.subscriptions[queue] = subscription_id
            logger.info(f"Subscribed to queue: {queue}")
            
        except Exception as e:
            logger.error(f"Subscription failed: {e}")
            raise
    
    def subscribe_to_topic(
        self,
        topic: str,
        callback: Callable,
        client_id: Optional[str] = None,
        durable: bool = False
    ):
        """Subscribe to topic."""
        try:
            subscription_id = str(uuid.uuid4())
            
            if durable and client_id:
                self.conn.set_client_id(client_id)
            
            self.conn.set_listener(
                subscription_id,
                TopicListener(callback)
            )
            
            if durable:
                self.conn.subscribe(
                    destination=f'/topic/{topic}',
                    id=subscription_id,
                    ack='client-individual',
                    headers={
                        'activemq.subscriptionName': client_id
                    }
                )
            else:
                self.conn.subscribe(
                    destination=f'/topic/{topic}',
                    id=subscription_id,
                    ack='auto'
                )
            
            self.subscriptions[topic] = subscription_id
            logger.info(f"Subscribed to topic: {topic}")
            
        except Exception as e:
            logger.error(f"Subscription failed: {e}")
            raise
    
    def unsubscribe(
        self,
        destination: str
    ):
        """Unsubscribe from queue or topic."""
        try:
            if destination in self.subscriptions:
                subscription_id = self.subscriptions[destination]
                self.conn.unsubscribe(subscription_id)
                del self.subscriptions[destination]
                logger.info(
                    f"Unsubscribed from: {destination}"
                )
            
        except Exception as e:
            logger.error(f"Unsubscribe failed: {e}")
            raise

class QueueListener(stomp.ConnectionListener):
    """Queue message listener."""
    
    def __init__(self, callback: Callable):
        """Initialize listener."""
        self.callback = callback
    
    def on_message(
        self,
        frame
    ):
        """Handle received message."""
        try:
            message = json.loads(frame.body)
            headers = frame.headers
            
            self.callback(message, headers)
            
        except Exception as e:
            logger.error(f"Message handling failed: {e}")

class TopicListener(stomp.ConnectionListener):
    """Topic message listener."""
    
    def __init__(self, callback: Callable):
        """Initialize listener."""
        self.callback = callback
    
    def on_message(
        self,
        frame
    ):
        """Handle received message."""
        try:
            message = json.loads(frame.body)
            headers = frame.headers
            
            self.callback(message, headers)
            
        except Exception as e:
            logger.error(f"Message handling failed: {e}")

def message_handler(
    message: Dict[str, Any],
    headers: Dict[str, str]
):
    """Example message handler."""
    logger.info(f"Received message: {message}")
    logger.info(f"Headers: {headers}")
    time.sleep(1)  # Simulate processing

def main():
    """Run ActiveMQ integration example."""
    client = ActiveMQClient()
    
    try:
        # Connect to broker
        client.connect()
        
        # Subscribe to queue
        client.subscribe_to_queue(
            'test.queue',
            message_handler
        )
        
        # Subscribe to topic
        client.subscribe_to_topic(
            'test.topic',
            message_handler,
            client_id='consumer1',
            durable=True
        )
        
        # Send messages
        client.send_to_queue(
            'test.queue',
            {'type': 'queue', 'id': 1}
        )
        
        client.send_to_topic(
            'test.topic',
            {'type': 'topic', 'id': 2}
        )
        
        # Keep connection alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        client.disconnect()

if __name__ == '__main__':
    main() 