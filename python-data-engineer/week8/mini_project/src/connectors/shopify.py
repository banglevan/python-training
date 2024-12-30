"""
Shopify connector implementation.
"""

import logging
from typing import Dict, Any, List, Optional, Iterator
from datetime import datetime
import hashlib
import json
import shopify
from tenacity import retry, stop_after_attempt, wait_exponential

from .base import DataConnector, SourceEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ShopifyConnector(DataConnector):
    """Shopify data source connector."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Shopify connector."""
        super().__init__(config)
        self.shop_url = config['shop_url']
        self.api_version = config.get('api_version', '2024-01')
        self.topics = config.get('topics', ['products/update', 'orders/create'])
        self.session = None
        self.webhook_token = None
    
    def initialize(self) -> bool:
        """Initialize Shopify session."""
        try:
            self.session = shopify.Session(
                self.shop_url,
                self.api_version,
                self.config['access_token']
            )
            shopify.ShopifyResource.activate_session(self.session)
            
            # Verify connection
            shop = shopify.Shop.current()
            logger.info(f"Connected to shop: {shop.name}")
            
            # Setup webhooks
            self._setup_webhooks()
            return True
            
        except Exception as e:
            logger.error(f"Shopify initialization failed: {e}")
            return False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _setup_webhooks(self):
        """Setup webhooks for topics."""
        try:
            # Remove existing webhooks
            existing = shopify.Webhook.find()
            for webhook in existing:
                webhook.destroy()
            
            # Create new webhooks
            for topic in self.topics:
                webhook = shopify.Webhook.create({
                    'topic': topic,
                    'address': self.config['webhook_url'],
                    'format': 'json'
                })
                
                if webhook.id:
                    logger.info(f"Created webhook for {topic}")
                    self.webhook_token = webhook.computed_hmac
                else:
                    logger.error(f"Failed to create webhook for {topic}")
            
        except Exception as e:
            logger.error(f"Webhook setup failed: {e}")
            raise
    
    def start(self) -> bool:
        """Start the connector."""
        try:
            if not self.session:
                return self.initialize()
            return True
            
        except Exception as e:
            logger.error(f"Start failed: {e}")
            return False
    
    def stop(self) -> bool:
        """Stop the connector."""
        try:
            if self.session:
                shopify.ShopifyResource.clear_session()
                self.session = None
            return True
            
        except Exception as e:
            logger.error(f"Stop failed: {e}")
            return False
    
    def get_events(self) -> Iterator[SourceEvent]:
        """Get events from Shopify."""
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
    
    def _get_next_event(self) -> Optional[Dict[str, Any]]:
        """Get next event from queue."""
        # Implementation depends on queue system
        # (Redis, RabbitMQ, etc.)
        pass
    
    def _create_event(self, data: Dict[str, Any]) -> SourceEvent:
        """Create source event from webhook data."""
        return SourceEvent(
            event_id=data['id'],
            source_type='SHOPIFY',
            source_id=self.shop_url,
            event_type=data['topic'],
            data=data['payload'],
            timestamp=datetime.fromisoformat(data['created_at']),
            checksum=self._generate_checksum(data['payload'])
        )
    
    def _generate_checksum(self, data: Dict[str, Any]) -> str:
        """Generate checksum for data."""
        return hashlib.sha256(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()
    
    def acknowledge_event(self, event_id: str) -> bool:
        """Acknowledge event processing."""
        try:
            # Mark event as processed in queue
            return True
            
        except Exception as e:
            logger.error(f"Event acknowledgment failed: {e}")
            return False
    
    def health_check(self) -> Dict[str, Any]:
        """Check connector health."""
        try:
            status = {
                'name': self.name,
                'type': 'SHOPIFY',
                'connected': False,
                'webhook_status': 'inactive',
                'last_check': datetime.now().isoformat()
            }
            
            if self.session:
                # Check API connection
                shop = shopify.Shop.current()
                status['connected'] = bool(shop)
                
                # Check webhooks
                webhooks = shopify.Webhook.find()
                status['webhook_status'] = 'active' if webhooks else 'inactive'
                status['webhook_count'] = len(webhooks)
            
            return status
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                'name': self.name,
                'type': 'SHOPIFY',
                'connected': False,
                'error': str(e),
                'last_check': datetime.now().isoformat()
            } 