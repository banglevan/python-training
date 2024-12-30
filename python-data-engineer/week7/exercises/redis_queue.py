"""
Redis Queue Exercise
----------------

Practice with:
1. Pub/Sub patterns
2. Stream operations
3. Consumer groups
"""

import redis
import json
import logging
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
import time
from contextlib import contextmanager
import threading
import queue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisQueue:
    """Redis queue operations handler."""
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None
    ):
        """Initialize Redis connection."""
        self.redis = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )
        
        # Track subscriptions
        self.pubsub = self.redis.pubsub(
            ignore_subscribe_messages=True
        )
        self.subscriber_threads = {}
        
        # Message buffer for subscribers
        self.message_buffer = queue.Queue()
    
    def publish(
        self,
        channel: str,
        message: Dict[str, Any]
    ):
        """Publish message to channel."""
        try:
            self.redis.publish(
                channel,
                json.dumps(message)
            )
            logger.info(
                f"Published message to channel: {channel}"
            )
            
        except Exception as e:
            logger.error(f"Message publish failed: {e}")
            raise
    
    def subscribe(
        self,
        channels: List[str],
        callback: Callable,
        pattern: bool = False
    ):
        """Subscribe to channels."""
        try:
            if pattern:
                self.pubsub.psubscribe(**{
                    ch: self._message_handler
                    for ch in channels
                })
            else:
                self.pubsub.subscribe(**{
                    ch: self._message_handler
                    for ch in channels
                })
            
            # Start subscriber thread
            thread = threading.Thread(
                target=self._subscriber_worker,
                args=(callback,),
                daemon=True
            )
            thread.start()
            
            # Track thread
            for channel in channels:
                self.subscriber_threads[channel] = thread
            
            logger.info(
                f"Subscribed to channels: {channels}"
            )
            
        except Exception as e:
            logger.error(f"Subscription failed: {e}")
            raise
    
    def _message_handler(
        self,
        message: Dict[str, Any]
    ):
        """Handle received message."""
        try:
            # Add to buffer
            self.message_buffer.put(message)
            
        except Exception as e:
            logger.error(
                f"Message handling failed: {e}"
            )
    
    def _subscriber_worker(
        self,
        callback: Callable
    ):
        """Worker thread for message processing."""
        try:
            while True:
                # Get message from pubsub
                message = self.pubsub.get_message()
                
                if message and message['type'] == 'message':
                    try:
                        # Process message
                        data = json.loads(
                            message['data']
                        )
                        callback(
                            message['channel'],
                            data
                        )
                        
                    except Exception as e:
                        logger.error(
                            f"Message processing failed: {e}"
                        )
                
                time.sleep(0.001)  # Prevent busy waiting
                
        except Exception as e:
            logger.error(f"Subscriber worker failed: {e}")
    
    def unsubscribe(
        self,
        channels: List[str],
        pattern: bool = False
    ):
        """Unsubscribe from channels."""
        try:
            if pattern:
                self.pubsub.punsubscribe(channels)
            else:
                self.pubsub.unsubscribe(channels)
            
            # Stop threads
            for channel in channels:
                if channel in self.subscriber_threads:
                    del self.subscriber_threads[channel]
            
            logger.info(
                f"Unsubscribed from channels: {channels}"
            )
            
        except Exception as e:
            logger.error(f"Unsubscribe failed: {e}")
            raise
    
    def list_push(
        self,
        key: str,
        value: Dict[str, Any]
    ):
        """Push value to list."""
        try:
            self.redis.lpush(
                key,
                json.dumps(value)
            )
            logger.info(f"Pushed value to list: {key}")
            
        except Exception as e:
            logger.error(f"List push failed: {e}")
            raise
    
    def list_pop(
        self,
        key: str,
        timeout: int = 0
    ) -> Optional[Dict[str, Any]]:
        """Pop value from list."""
        try:
            if timeout > 0:
                result = self.redis.brpop(
                    key,
                    timeout
                )
                if result:
                    return json.loads(result[1])
            else:
                result = self.redis.rpop(key)
                if result:
                    return json.loads(result)
            
            return None
            
        except Exception as e:
            logger.error(f"List pop failed: {e}")
            raise
    
    def close(self):
        """Clean up resources."""
        try:
            # Unsubscribe all
            self.pubsub.close()
            
            # Close Redis connection
            self.redis.close()
            
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")

def message_handler(
    channel: str,
    message: Dict[str, Any]
):
    """Example message handler."""
    logger.info(
        f"Received message from {channel}: {message}"
    )

def main():
    """Run Redis queue example."""
    queue = RedisQueue()
    
    try:
        # Subscribe to channels
        queue.subscribe(
            ['test_channel', 'alerts.*'],
            message_handler,
            pattern=True
        )
        
        # Publish messages
        for i in range(5):
            queue.publish(
                'test_channel',
                {
                    'id': i,
                    'data': f"Test message {i}",
                    'timestamp': datetime.now().isoformat()
                }
            )
            
            queue.publish(
                f"alerts.priority_{i}",
                {
                    'level': i,
                    'message': f"Alert {i}",
                    'timestamp': datetime.now().isoformat()
                }
            )
        
        # Use list operations
        list_key = 'test_list'
        
        # Push items
        for i in range(3):
            queue.list_push(
                list_key,
                {
                    'id': i,
                    'data': f"List item {i}"
                }
            )
        
        # Pop items
        while True:
            item = queue.list_pop(list_key)
            if not item:
                break
            logger.info(f"Popped item: {item}")
        
        # Keep running to receive messages
        time.sleep(10)
        
    finally:
        queue.close()

if __name__ == '__main__':
    main() 