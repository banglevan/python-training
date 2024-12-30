"""
Elasticsearch index mappings.
"""

import requests
import json
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ElasticsearchManager:
    """Elasticsearch index manager."""
    
    def __init__(self, host: str = "http://elasticsearch:9200"):
        """Initialize manager."""
        self.host = host
    
    def create_index(self, index: str, mapping: Dict[str, Any]) -> None:
        """Create Elasticsearch index."""
        try:
            url = f"{self.host}/{index}"
            
            # Create index with mapping
            response = requests.put(
                url,
                json=mapping,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            
            logger.info(f"Created index: {index}")
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                logger.warning(f"Index already exists: {index}")
            else:
                logger.error(f"Failed to create index: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to create index: {e}")
            raise
    
    def create_all_indices(self):
        """Create all required indices."""
        # Sales metrics mapping
        sales_mapping = {
            "mappings": {
                "properties": {
                    "window": {
                        "properties": {
                            "start": {"type": "date"},
                            "end": {"type": "date"}
                        }
                    },
                    "product_id": {"type": "keyword"},
                    "total_quantity": {"type": "long"},
                    "total_revenue": {"type": "double"},
                    "num_purchases": {"type": "long"}
                }
            },
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 1
            }
        }
        
        # User metrics mapping
        user_mapping = {
            "mappings": {
                "properties": {
                    "window": {
                        "properties": {
                            "start": {"type": "date"},
                            "end": {"type": "date"}
                        }
                    },
                    "user_id": {"type": "integer"},
                    "num_sessions": {"type": "long"},
                    "products_viewed": {"type": "long"},
                    "num_purchases": {"type": "long"}
                }
            },
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 1
            }
        }
        
        # Product metrics mapping
        product_mapping = {
            "mappings": {
                "properties": {
                    "window": {
                        "properties": {
                            "start": {"type": "date"},
                            "end": {"type": "date"}
                        }
                    },
                    "product_id": {"type": "keyword"},
                    "views": {"type": "long"},
                    "cart_adds": {"type": "long"},
                    "purchases": {"type": "long"}
                }
            },
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 1
            }
        }
        
        # Create indices
        indices = {
            "metrics_sales": sales_mapping,
            "metrics_users": user_mapping,
            "metrics_products": product_mapping
        }
        
        for index, mapping in indices.items():
            self.create_index(index, mapping)

if __name__ == "__main__":
    manager = ElasticsearchManager()
    manager.create_all_indices() 