"""
E-commerce MongoDB Manager
------------------------

PURPOSE:
Handles all product and customer data operations:
1. Product Catalog
   - Product details
   - Categories
   - Pricing history
   
2. Customer Profiles
   - Personal info
   - Shopping history
   - Preferences

3. Order Management
   - Order details
   - Payment info
   - Shipping status
"""

from typing import Dict, List, Any
from pymongo import MongoClient
import logging
from datetime import datetime
from .base_manager import BaseManager

logger = logging.getLogger(__name__)

class EcommerceMongoManager(BaseManager):
    """MongoDB manager for e-commerce data."""
    
    def _connect(self) -> None:
        """Initialize MongoDB connection."""
        try:
            self.client = MongoClient(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 27017)
            )
            self.db = self.client.ecommerce
            
            # Initialize collections
            self.products = self.db.products
            self.customers = self.db.customers
            self.orders = self.db.orders
            
            # Create indexes
            self._setup_indexes()
            logger.info("E-commerce MongoDB initialized")
            
        except Exception as e:
            logger.error(f"MongoDB initialization error: {e}")
            raise
    
    def _setup_indexes(self) -> None:
        """Setup necessary indexes."""
        # Product indexes
        self.products.create_index([("sku", 1)], unique=True)
        self.products.create_index([("category", 1)])
        self.products.create_index([("name", "text")])
        
        # Customer indexes
        self.customers.create_index([("email", 1)], unique=True)
        self.customers.create_index([("customer_id", 1)], unique=True)
        
        # Order indexes
        self.orders.create_index([("customer_id", 1)])
        self.orders.create_index([("order_date", -1)])
    
    def add_product(self, product: Dict) -> str:
        """Add new product to catalog."""
        try:
            product['created_at'] = datetime.utcnow()
            product['updated_at'] = datetime.utcnow()
            
            result = self.products.insert_one(product)
            logger.info(f"Added product: {result.inserted_id}")
            return str(result.inserted_id)
            
        except Exception as e:
            logger.error(f"Error adding product: {e}")
            raise
    
    def update_product(
        self,
        product_id: str,
        updates: Dict
    ) -> bool:
        """Update product details."""
        try:
            updates['updated_at'] = datetime.utcnow()
            
            result = self.products.update_one(
                {"_id": product_id},
                {"$set": updates}
            )
            
            success = result.modified_count > 0
            if success:
                logger.info(f"Updated product: {product_id}")
            return success
            
        except Exception as e:
            logger.error(f"Error updating product: {e}")
            raise
    
    def get_customer_history(
        self,
        customer_id: str
    ) -> Dict[str, Any]:
        """Get customer's complete shopping history."""
        try:
            pipeline = [
                {"$match": {"customer_id": customer_id}},
                {"$lookup": {
                    "from": "orders",
                    "localField": "customer_id",
                    "foreignField": "customer_id",
                    "as": "orders"
                }},
                {"$project": {
                    "profile": 1,
                    "total_orders": {"$size": "$orders"},
                    "total_spent": {
                        "$sum": "$orders.total_amount"
                    },
                    "last_order_date": {
                        "$max": "$orders.order_date"
                    }
                }}
            ]
            
            result = list(
                self.customers.aggregate(pipeline)
            )
            return result[0] if result else None
            
        except Exception as e:
            logger.error(
                f"Error fetching customer history: {e}"
            )
            raise
    
    def _disconnect(self) -> None:
        """Close MongoDB connection."""
        if hasattr(self, 'client'):
            self.client.close()
    
    def health_check(self) -> Dict[str, Any]:
        """Check MongoDB connection health."""
        try:
            self.client.admin.command('ping')
            return {
                'status': 'healthy',
                'timestamp': datetime.utcnow()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.utcnow()
            } 