"""
RabbitMQ Operations Exercise
-------------------------

Practice with:
1. Queue management
2. Exchange types
3. Routing strategies
4. Dead letter queues
"""

import pika
import json
import logging
from typing import Dict, Any, Optional, Callable
from datetime import datetime
import time
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RabbitMQOperator:
    """RabbitMQ operations handler."""
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5672,
        username: str = 'guest',
        password: str = 'guest',
        virtual_host: str = '/'
    ):
        """Initialize RabbitMQ connection."""
        self.credentials = pika.PlainCredentials(
            username,
            password
        )
        self.parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=self.credentials
        )
        self.connection = None
        self.channel = None
    
    @contextmanager
    def connection_channel(self):
        """Context manager for connection and channel."""
        try:
            self.connection = pika.BlockingConnection(
                self.parameters
            )
            self.channel = self.connection.channel()
            yield self.channel
        finally:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
    
    def declare_exchange(
        self,
        exchange_name: str,
        exchange_type: str = 'direct',
        durable: bool = True
    ):
        """Declare exchange with specified type."""
        with self.connection_channel() as channel:
            channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=durable
            )
            logger.info(
                f"Declared {exchange_type} "
                f"exchange: {exchange_name}"
            )
    
    def declare_queue(
        self,
        queue_name: str,
        durable: bool = True,
        dead_letter_exchange: Optional[str] = None,
        message_ttl: Optional[int] = None
    ):
        """Declare queue with optional DLX and TTL."""
        with self.connection_channel() as channel:
            arguments = {}
            
            if dead_letter_exchange:
                arguments['x-dead-letter-exchange'] = (
                    dead_letter_exchange
                )
            
            if message_ttl:
                arguments['x-message-ttl'] = message_ttl
            
            channel.queue_declare(
                queue=queue_name,
                durable=durable,
                arguments=arguments
            )
            logger.info(f"Declared queue: {queue_name}")
    
    def bind_queue(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str
    ):
        """Bind queue to exchange with routing key."""
        with self.connection_channel() as channel:
            channel.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key
            )
            logger.info(
                f"Bound queue {queue_name} to exchange "
                f"{exchange_name} with key {routing_key}"
            )
    
    def publish_message(
        self,
        exchange_name: str,
        routing_key: str,
        message: Dict[str, Any],
        persistent: bool = True
    ):
        """Publish message to exchange."""
        with self.connection_channel() as channel:
            properties = pika.BasicProperties(
                delivery_mode=2 if persistent else 1,
                timestamp=int(time.time()),
                content_type='application/json'
            )
            
            channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=json.dumps(message).encode(),
                properties=properties
            )
            logger.info(
                f"Published message to {exchange_name} "
                f"with key {routing_key}"
            )
    
    def consume_messages(
        self,
        queue_name: str,
        callback: Callable,
        auto_ack: bool = False
    ):
        """Consume messages from queue."""
        with self.connection_channel() as channel:
            channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=auto_ack
            )
            logger.info(
                f"Started consuming from queue: {queue_name}"
            )
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                channel.stop_consuming()
    
    def setup_dead_letter_queue(
        self,
        queue_name: str,
        dlx_name: str = 'dlx',
        dlq_name: str = 'dlq',
        message_ttl: int = 30000
    ):
        """Set up dead letter exchange and queue."""
        # Declare DLX
        self.declare_exchange(
            dlx_name,
            exchange_type='direct'
        )
        
        # Declare DLQ
        self.declare_queue(dlq_name)
        
        # Bind DLQ to DLX
        self.bind_queue(
            dlq_name,
            dlx_name,
            routing_key=queue_name
        )
        
        # Declare main queue with DLX
        self.declare_queue(
            queue_name,
            dead_letter_exchange=dlx_name,
            message_ttl=message_ttl
        )
        
        logger.info(
            f"Set up DLQ system for queue: {queue_name}"
        )

def message_handler(
    ch,
    method,
    properties,
    body
):
    """Example message handler."""
    try:
        message = json.loads(body)
        logger.info(f"Received message: {message}")
        
        # Process message here
        time.sleep(1)  # Simulate processing
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Message processed successfully")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=False
        )

def main():
    """Run RabbitMQ operations example."""
    operator = RabbitMQOperator()
    
    # Set up exchanges
    operator.declare_exchange(
        'direct_exchange',
        'direct'
    )
    operator.declare_exchange(
        'topic_exchange',
        'topic'
    )
    operator.declare_exchange(
        'fanout_exchange',
        'fanout'
    )
    
    # Set up queues with DLQ
    operator.setup_dead_letter_queue('queue1')
    operator.setup_dead_letter_queue('queue2')
    
    # Bind queues to exchanges
    operator.bind_queue(
        'queue1',
        'direct_exchange',
        'key1'
    )
    operator.bind_queue(
        'queue2',
        'topic_exchange',
        'orders.#'
    )
    operator.bind_queue(
        'queue1',
        'fanout_exchange',
        ''
    )
    operator.bind_queue(
        'queue2',
        'fanout_exchange',
        ''
    )
    
    # Publish messages
    operator.publish_message(
        'direct_exchange',
        'key1',
        {'type': 'direct', 'id': 1}
    )
    operator.publish_message(
        'topic_exchange',
        'orders.new',
        {'type': 'topic', 'id': 2}
    )
    operator.publish_message(
        'fanout_exchange',
        '',
        {'type': 'fanout', 'id': 3}
    )
    
    # Consume messages
    operator.consume_messages(
        'queue1',
        message_handler
    )

if __name__ == '__main__':
    main() 