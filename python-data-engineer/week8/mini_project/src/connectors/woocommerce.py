"""
WooCommerce connector implementation.
"""

import logging
from typing import Dict, Any, List, Optional, Iterator
from datetime import datetime
import hashlib
import json
from woocommerce import API
from tenacity import retry, stop_after_attempt, wait_exponential

from .base import DataConnector, SourceEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WooCommerceConnector(DataConnector):
    """WooCommerce data source connector."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize WooCommerce connector."""
        super().__init__(config)
        self.site_url = config['site_url']
        self.api_version = config.get('api_version', 'wc/v3')
        self.webhook_secret = None
        self.wcapi = None
    
    def initialize(self) -> bool:
        """Initialize WooCommerce connection."""
        try:
            self.wcapi = API(
                url=self.site_url,
                consumer_key=self.config['consumer_key'],
                consumer_secret=self.config['consumer_secret'],
                version=self.api_version
            )
            
            # Verify connection
            response = self.wcapi.get("system_status")
            if response.status_code == 200:
                logger.info("Connected to WooCommerce site")
                self._setup_webhooks()
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"WooCommerce initialization failed: {e}")
            return False
    
    def get_products(self, filters: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """
        Get products with filters.
        
        Args:
            filters: Dict of filter parameters:
                - category_id: Product category ID
                - tag_id: Product tag ID
                - status: Product status
                - stock_status: in_stock, out_of_stock, on_backorder
                - price_range: Dict with min_price and max_price
                - date_range: Dict with start_date and end_date
        """
        try:
            params = {}
            
            if 'category_id' in filters:
                params['category'] = filters['category_id']
            
            if 'tag_id' in filters:
                params['tag'] = filters['tag_id']
            
            if 'status' in filters:
                params['status'] = filters['status']
            
            if 'stock_status' in filters:
                params['stock_status'] = filters['stock_status']
            
            if 'price_range' in filters:
                price_range = filters['price_range']
                if 'min_price' in price_range:
                    params['min_price'] = price_range['min_price']
                if 'max_price' in price_range:
                    params['max_price'] = price_range['max_price']
            
            if 'date_range' in filters:
                date_range = filters['date_range']
                if 'start_date' in date_range:
                    params['after'] = date_range['start_date']
                if 'end_date' in date_range:
                    params['before'] = date_range['end_date']
            
            response = self.wcapi.get("products", params=params)
            
            if response.status_code == 200:
                return response.json()
            
            logger.error(
                f"Failed to get products. Status: {response.status_code}"
            )
            return None
            
        except Exception as e:
            logger.error(f"Failed to get products: {e}")
            return None
    
    def get_orders(
        self,
        status: Optional[str] = None,
        date_range: Optional[Dict[str, str]] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get orders with optional filters.
        
        Args:
            status: Order status (processing, completed, etc.)
            date_range: Dict with start_date and end_date
        """
        try:
            params = {}
            
            if status:
                params['status'] = status
            
            if date_range:
                if 'start_date' in date_range:
                    params['after'] = date_range['start_date']
                if 'end_date' in date_range:
                    params['before'] = date_range['end_date']
            
            response = self.wcapi.get("orders", params=params)
            
            if response.status_code == 200:
                return response.json()
            
            logger.error(
                f"Failed to get orders. Status: {response.status_code}"
            )
            return None
            
        except Exception as e:
            logger.error(f"Failed to get orders: {e}")
            return None
    
    def get_events(self) -> Iterator[SourceEvent]:
        """Get events from WooCommerce webhooks."""
        try:
            # Process webhook events from queue
            while True:
                event_data = self._get_next_event()
                if not event_data:
                    break
                
                yield self._create_event(event_data)
                
        except Exception as e:
            logger.error(f"Event processing failed: {e}")
            raise
    
    def health_check(self) -> Dict[str, Any]:
        """Check connector health."""
        try:
            status = {
                'name': self.name,
                'type': 'WOOCOMMERCE',
                'connected': False,
                'webhook_status': 'inactive',
                'last_check': datetime.now().isoformat()
            }
            
            if self.wcapi:
                response = self.wcapi.get("system_status")
                status['connected'] = response.status_code == 200
                
                if status['connected']:
                    webhooks = self.wcapi.get("webhooks").json()
                    status['webhook_status'] = (
                        'active' if webhooks else 'inactive'
                    )
                    status['webhook_count'] = len(webhooks)
            
            return status
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                'name': self.name,
                'type': 'WOOCOMMERCE',
                'connected': False,
                'error': str(e),
                'last_check': datetime.now().isoformat()
            } 