"""
Initialize Kafka topics.
"""

import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_topics(bootstrap_servers: list, topics: list) -> None:
    """Create Kafka topics."""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='admin_client'
        )
        
        # Create topic configurations
        topic_list = [
            NewTopic(
                name=topic['name'],
                num_partitions=topic['partitions'],
                replication_factor=topic['replication']
            )
            for topic in topics
        ]
        
        # Create topics
        admin_client.create_topics(topic_list)
        logger.info(f"Created topics: {[t['name'] for t in topics]}")
        
    except TopicAlreadyExistsError:
        logger.warning("Topics already exist")
    except Exception as e:
        logger.error(f"Failed to create topics: {e}")
        raise
    finally:
        admin_client.close()

if __name__ == "__main__":
    # Define topics
    topics = [
        {
            'name': 'ecommerce_events',
            'partitions': 3,
            'replication': 1
        },
        {
            'name': 'processed_events',
            'partitions': 3,
            'replication': 1
        },
        {
            'name': 'alerts',
            'partitions': 1,
            'replication': 1
        }
    ]
    
    # Create topics
    create_topics(['localhost:9092'], topics) 