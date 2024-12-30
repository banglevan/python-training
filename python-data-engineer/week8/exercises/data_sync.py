"""
Data Sync Implementation
-------------------

Real Product Example: Shopify Multi-Store Sync
- Syncs product data across multiple Shopify stores
- Handles inventory, pricing, and metadata updates
- Resolves conflicts using timestamp-based strategy
- Implements recovery for failed syncs
"""

import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
import json
import time
import hashlib
from dataclasses import dataclass
import requests
from redis import Redis
from sqlalchemy import create_engine, text
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SyncEvent:
    """Sync event structure."""
    store_id: str
    product_id: str
    event_type: str
    data: Dict[str, Any]
    timestamp: datetime
    checksum: str

class ShopifySync:
    """Shopify multi-store sync implementation."""
    
    def __init__(
        self,
        stores_config: Dict[str, Dict[str, str]],
        db_url: str,
        redis_url: str
    ):
        """Initialize sync manager."""
        self.stores = stores_config
        self.db = create_engine(db_url)
        self.redis = Redis.from_url(redis_url)
        
        # Create tables
        self._init_tables()
    
    def _init_tables(self):
        """Initialize database tables."""
        with self.db.connect() as conn:
            # Sync events table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sync_events (
                    id SERIAL PRIMARY KEY,
                    store_id VARCHAR(50),
                    product_id VARCHAR(50),
                    event_type VARCHAR(20),
                    data JSONB,
                    timestamp TIMESTAMP,
                    checksum VARCHAR(64),
                    status VARCHAR(20) DEFAULT 'pending'
                )
            """))
            
            # Conflict log table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS conflict_log (
                    id SERIAL PRIMARY KEY,
                    event_id INTEGER REFERENCES sync_events(id),
                    conflict_type VARCHAR(50),
                    resolution VARCHAR(50),
                    resolved_at TIMESTAMP
                )
            """))
            
            conn.commit()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _fetch_product(
        self,
        store_id: str,
        product_id: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch product from Shopify store."""
        store_config = self.stores[store_id]
        
        response = requests.get(
            f"https://{store_config['shop_url']}/admin/api/2024-01/products/{product_id}.json",
            headers={
                'X-Shopify-Access-Token': store_config['access_token']
            }
        )
        
        if response.status_code == 200:
            return response.json()['product']
        return None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _update_product(
        self,
        store_id: str,
        product_id: str,
        data: Dict[str, Any]
    ) -> bool:
        """Update product in Shopify store."""
        store_config = self.stores[store_id]
        
        response = requests.put(
            f"https://{store_config['shop_url']}/admin/api/2024-01/products/{product_id}.json",
            headers={
                'X-Shopify-Access-Token': store_config['access_token'],
                'Content-Type': 'application/json'
            },
            json={'product': data}
        )
        
        return response.status_code == 200
    
    def _generate_checksum(self, data: Dict[str, Any]) -> str:
        """Generate checksum for data."""
        return hashlib.sha256(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()
    
    def sync_product(
        self,
        source_store: str,
        product_id: str,
        target_stores: Optional[List[str]] = None
    ):
        """Sync product across stores."""
        try:
            # Fetch product data
            product = self._fetch_product(source_store, product_id)
            if not product:
                raise ValueError(f"Product {product_id} not found")
            
            # Generate sync event
            event = SyncEvent(
                store_id=source_store,
                product_id=product_id,
                event_type='update',
                data=product,
                timestamp=datetime.now(),
                checksum=self._generate_checksum(product)
            )
            
            # Store sync event
            with self.db.connect() as conn:
                result = conn.execute(text("""
                    INSERT INTO sync_events
                    (store_id, product_id, event_type, data, timestamp, checksum)
                    VALUES (:store_id, :product_id, :event_type, :data, :timestamp, :checksum)
                    RETURNING id
                """), {
                    'store_id': event.store_id,
                    'product_id': event.product_id,
                    'event_type': event.event_type,
                    'data': json.dumps(event.data),
                    'timestamp': event.timestamp,
                    'checksum': event.checksum
                })
                event_id = result.scalar_one()
                
                # Sync to target stores
                target_stores = target_stores or [
                    s for s in self.stores.keys()
                    if s != source_store
                ]
                
                for store in target_stores:
                    try:
                        # Check for conflicts
                        target_product = self._fetch_product(
                            store,
                            product_id
                        )
                        
                        if target_product:
                            target_updated = datetime.fromisoformat(
                                target_product['updated_at']
                            )
                            source_updated = datetime.fromisoformat(
                                product['updated_at']
                            )
                            
                            if target_updated > source_updated:
                                # Log conflict
                                conn.execute(text("""
                                    INSERT INTO conflict_log
                                    (event_id, conflict_type, resolution, resolved_at)
                                    VALUES (:event_id, :conflict_type, :resolution, :resolved_at)
                                """), {
                                    'event_id': event_id,
                                    'conflict_type': 'timestamp_conflict',
                                    'resolution': 'skipped',
                                    'resolved_at': datetime.now()
                                })
                                continue
                        
                        # Update product
                        if self._update_product(store, product_id, product):
                            logger.info(
                                f"Synced product {product_id} to store {store}"
                            )
                        
                    except Exception as e:
                        logger.error(
                            f"Sync failed for store {store}: {e}"
                        )
                        # Log failure
                        conn.execute(text("""
                            INSERT INTO conflict_log
                            (event_id, conflict_type, resolution, resolved_at)
                            VALUES (:event_id, :conflict_type, :resolution, :resolved_at)
                        """), {
                            'event_id': event_id,
                            'conflict_type': 'sync_error',
                            'resolution': str(e),
                            'resolved_at': datetime.now()
                        })
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            raise
    
    def retry_failed_syncs(self):
        """Retry failed sync events."""
        with self.db.connect() as conn:
            # Get failed events
            failed_events = conn.execute(text("""
                SELECT e.*, c.conflict_type
                FROM sync_events e
                JOIN conflict_log c ON c.event_id = e.id
                WHERE c.conflict_type = 'sync_error'
                ORDER BY e.timestamp DESC
            """))
            
            for event in failed_events:
                try:
                    # Retry sync
                    self.sync_product(
                        event.store_id,
                        event.product_id
                    )
                    logger.info(
                        f"Retried sync for product {event.product_id}"
                    )
                    
                except Exception as e:
                    logger.error(
                        f"Retry failed for product {event.product_id}: {e}"
                    )
    
    def close(self):
        """Close connections."""
        self.redis.close()

def main():
    """Run data sync example."""
    # Configuration
    stores_config = {
        'store1': {
            'shop_url': 'store1.myshopify.com',
            'access_token': 'token1'
        },
        'store2': {
            'shop_url': 'store2.myshopify.com',
            'access_token': 'token2'
        }
    }
    
    sync = ShopifySync(
        stores_config,
        'postgresql://user:pass@localhost/syncdb',
        'redis://localhost:6379/0'
    )
    
    try:
        # Sync product
        sync.sync_product(
            'store1',
            '123456789',
            ['store2']
        )
        
        # Retry failed syncs
        sync.retry_failed_syncs()
        
    finally:
        sync.close()

if __name__ == '__main__':
    main() 