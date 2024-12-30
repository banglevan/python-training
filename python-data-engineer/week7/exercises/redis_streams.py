"""
Redis Streams Exercise
------------------

Practice with:
1. Stream management
2. Consumer groups
3. Message acknowledgment
"""

import redis
import json
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import time
from contextlib import contextmanager
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisStreams:
    """Redis streams handler."""
    
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
        
        # Track consumer groups
        self.consumer_threads = {}
    
    def create_stream(
        self,
        stream: str,
        max_length: Optional[int] = None,
        approximate: bool = True
    ):
        """Create stream with optional length limit."""
        try:
            # Add dummy message to create stream
            message_id = self.add_message(
                stream,
                {'_init': True}
            )
            
            # Set length limit if specified
            if max_length:
                self.redis.xtrim(
                    stream,
                    maxlen=max_length,
                    approximate=approximate
                )
            
            # Delete dummy message
            self.redis.xdel(stream, message_id)
            
            logger.info(f"Created stream: {stream}")
            
        except Exception as e:
            logger.error(f"Stream creation failed: {e}")
            raise
    
    def add_message(
        self,
        stream: str,
        message: Dict[str, Any],
        max_length: Optional[int] = None
    ) -> str:
        """Add message to stream."""
        try:
            # Convert message to string values
            fields = {
                k: str(v)
                for k, v in message.items()
            }
            
            # Add message
            message_id = self.redis.xadd(
                stream,
                fields,
                maxlen=max_length
            )
            
            logger.info(
                f"Added message {message_id} to stream: {stream}"
            )
            return message_id
            
        except Exception as e:
            logger.error(f"Message add failed: {e}")
            raise
    
    def create_consumer_group(
        self,
        stream: str,
        group: str,
        start_id: str = '0'
    ):
        """Create consumer group."""
        try:
            self.redis.xgroup_create(
                stream,
                group,
                start_id,
                mkstream=True
            )
            logger.info(
                f"Created consumer group {group} "
                f"for stream: {stream}"
            )
            
        except redis.exceptions.ResponseError as e:
            if 'BUSYGROUP' in str(e):
                logger.info(
                    f"Consumer group {group} already exists"
                )
            else:
                raise
        except Exception as e:
            logger.error(
                f"Consumer group creation failed: {e}"
            )
            raise
    
    def read_messages(
        self,
        streams: Dict[str, str],
        count: Optional[int] = None,
        block: Optional[int] = None
    ) -> Dict[str, List[Tuple[str, Dict]]]:
        """Read messages from streams."""
        try:
            messages = self.redis.xread(
                streams,
                count=count,
                block=block
            )
            
            if messages:
                # Convert to more friendly format
                result = {}
                for stream, msgs in messages:
                    result[stream] = [
                        (mid, msg)
                        for mid, msg in msgs
                    ]
                return result
            
            return {}
            
        except Exception as e:
            logger.error(f"Message read failed: {e}")
            raise
    
    def read_group(
        self,
        group: str,
        consumer: str,
        streams: Dict[str, str],
        count: Optional[int] = None,
        block: Optional[int] = None,
        noack: bool = False
    ) -> Dict[str, List[Tuple[str, Dict]]]:
        """Read messages as consumer group member."""
        try:
            messages = self.redis.xreadgroup(
                group,
                consumer,
                streams,
                count=count,
                block=block,
                noack=noack
            )
            
            if messages:
                # Convert to more friendly format
                result = {}
                for stream, msgs in messages:
                    result[stream] = [
                        (mid, msg)
                        for mid, msg in msgs
                    ]
                return result
            
            return {}
            
        except Exception as e:
            logger.error(
                f"Consumer group read failed: {e}"
            )
            raise
    
    def acknowledge_message(
        self,
        stream: str,
        group: str,
        message_id: str
    ):
        """Acknowledge message processing."""
        try:
            self.redis.xack(stream, group, message_id)
            logger.info(
                f"Acknowledged message {message_id} "
                f"for group {group}"
            )
            
        except Exception as e:
            logger.error(f"Message ack failed: {e}")
            raise
    
    def claim_pending_messages(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        count: Optional[int] = None
    ) -> List[Tuple[str, Dict]]:
        """Claim pending messages from other consumers."""
        try:
            # Get pending message IDs
            pending = self.redis.xpending_range(
                stream,
                group,
                min_idle_time=min_idle_time,
                count=count
            )
            
            if not pending:
                return []
            
            # Claim messages
            message_ids = [p['message_id'] for p in pending]
            messages = self.redis.xclaim(
                stream,
                group,
                consumer,
                min_idle_time,
                message_ids
            )
            
            return [
                (mid, msg)
                for mid, msg in messages
            ]
            
        except Exception as e:
            logger.error(f"Message claim failed: {e}")
            raise
    
    def start_consumer(
        self,
        stream: str,
        group: str,
        consumer: str,
        callback,
        batch_size: int = 10,
        block: int = 1000
    ):
        """Start consumer thread."""
        try:
            # Create consumer group if needed
            self.create_consumer_group(stream, group)
            
            # Start consumer thread
            thread = threading.Thread(
                target=self._consumer_worker,
                args=(
                    stream,
                    group,
                    consumer,
                    callback,
                    batch_size,
                    block
                ),
                daemon=True
            )
            thread.start()
            
            # Track thread
            self.consumer_threads[
                f"{stream}:{group}:{consumer}"
            ] = thread
            
            logger.info(
                f"Started consumer {consumer} in group {group}"
            )
            
        except Exception as e:
            logger.error(f"Consumer start failed: {e}")
            raise
    
    def _consumer_worker(
        self,
        stream: str,
        group: str,
        consumer: str,
        callback,
        batch_size: int,
        block: int
    ):
        """Consumer worker thread."""
        try:
            while True:
                try:
                    # Read new messages
                    messages = self.read_group(
                        group,
                        consumer,
                        {stream: '>'},
                        count=batch_size,
                        block=block
                    )
                    
                    if stream in messages:
                        for msg_id, msg in messages[stream]:
                            try:
                                # Process message
                                callback(msg)
                                
                                # Acknowledge
                                self.acknowledge_message(
                                    stream,
                                    group,
                                    msg_id
                                )
                                
                            except Exception as e:
                                logger.error(
                                    f"Message processing "
                                    f"failed: {e}"
                                )
                    
                    # Claim pending messages
                    pending = self.claim_pending_messages(
                        stream,
                        group,
                        consumer,
                        min_idle_time=30000,  # 30s
                        count=batch_size
                    )
                    
                    for msg_id, msg in pending:
                        try:
                            # Reprocess message
                            callback(msg)
                            
                            # Acknowledge
                            self.acknowledge_message(
                                stream,
                                group,
                                msg_id
                            )
                            
                        except Exception as e:
                            logger.error(
                                f"Pending message "
                                f"processing failed: {e}"
                            )
                    
                except Exception as e:
                    logger.error(
                        f"Consumer worker error: {e}"
                    )
                    time.sleep(1)
                
        except Exception as e:
            logger.error(f"Consumer worker failed: {e}")
    
    def close(self):
        """Clean up resources."""
        try:
            # Close Redis connection
            self.redis.close()
            
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")

def message_handler(message: Dict[str, str]):
    """Example message handler."""
    logger.info(f"Processing message: {message}")
    time.sleep(0.1)  # Simulate processing

def main():
    """Run Redis streams example."""
    streams = RedisStreams()
    
    try:
        stream_name = 'test_stream'
        group_name = 'test_group'
        
        # Create stream
        streams.create_stream(
            stream_name,
            max_length=1000
        )
        
        # Start consumers
        for i in range(3):
            streams.start_consumer(
                stream_name,
                group_name,
                f"consumer_{i}",
                message_handler
            )
        
        # Produce messages
        for i in range(10):
            streams.add_message(
                stream_name,
                {
                    'id': i,
                    'data': f"Test message {i}",
                    'timestamp': datetime.now().isoformat()
                }
            )
        
        # Keep running to process messages
        time.sleep(10)
        
    finally:
        streams.close()

if __name__ == '__main__':
    main() 