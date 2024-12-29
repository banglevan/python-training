"""
E-commerce Multi-Database System Coordinator
-----------------------------------------

PURPOSE:
Coordinates operations across different databases:
1. System Integration
   - Database synchronization
   - Transaction management
   - Data consistency

2. Business Operations
   - Order processing
   - Inventory management
   - Customer analytics

3. Performance Optimization
   - Query routing
   - Cache management
   - Load balancing

ARCHITECTURE:
------------
1. MongoDB: Product & Customer Data
2. Redis: Cart & Sessions
3. TimescaleDB: Analytics & Metrics
4. Neo4j: Recommendations & Patterns
"""

from typing import Dict, List, Any, Optional
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import json

from db_managers.ecommerce_mongo import EcommerceMongoManager
from db_managers.ecommerce_redis import EcommerceRedisManager
from db_managers.ecommerce_timescale import EcommerceTimescaleManager
from db_managers.ecommerce_neo4j import EcommerceNeo4jManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EcommerceSystem:
    """Main coordinator for e-commerce system."""
    
    def __init__(self, config: Dict[str, Dict[str, Any]]):
        """Initialize all database managers."""
        try:
            # Initialize database managers
            self.mongo = EcommerceMongoManager(config.get('mongo', {}))
            self.redis = EcommerceRedisManager(config.get('redis', {}))
            self.timescale = EcommerceTimescaleManager(config.get('timescale', {}))
            self.neo4j = EcommerceNeo4jManager(config.get('neo4j', {}))
            
            # Thread pool for async operations
            self.executor = ThreadPoolExecutor(max_workers=4)
            
            logger.info("E-commerce system initialized")
            
        except Exception as e:
            logger.error(f"System initialization error: {e}")
            raise
    
    def process_order(
        self,
        customer_id: str,
        items: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Process a complete order transaction."""
        try:
            # 1. Validate cart and stock
            cart = self.redis.get_cart(customer_id)
            if not cart:
                raise ValueError("Empty cart")
            
            for item in items:
                if not self.redis.check_stock(
                    item['product_id'],
                    item['quantity']
                ):
                    raise ValueError(
                        f"Insufficient stock for product: {item['product_id']}"
                    )
            
            # 2. Create order in MongoDB
            order_data = {
                'customer_id': customer_id,
                'items': items,
                'total_amount': sum(
                    item['price'] * item['quantity']
                    for item in items
                ),
                'status': 'processing',
                'created_at': datetime.utcnow()
            }
            
            order_id = self.mongo.add_order(order_data)
            
            # 3. Update inventory (Redis & TimescaleDB)
            for item in items:
                # Update Redis stock
                self.redis.update_stock(
                    item['product_id'],
                    item['quantity'],
                    'decrease'
                )
                
                # Record in TimescaleDB
                self.timescale.record_sale({
                    'time': datetime.utcnow(),
                    'product_id': item['product_id'],
                    'customer_id': customer_id,
                    'quantity': item['quantity'],
                    'unit_price': item['price'],
                    'total_amount': item['price'] * item['quantity'],
                    'order_id': order_id,
                    'store_id': 'online'
                })
                
                # Update Neo4j purchase pattern
                self.neo4j.record_purchase(
                    customer_id,
                    item['product_id'],
                    {
                        'order_id': order_id,
                        'quantity': item['quantity'],
                        'amount': item['price'] * item['quantity']
                    }
                )
            
            # 4. Clear cart
            self.redis.manage_cart(customer_id, 'clear')
            
            # 5. Update order status
            self.mongo.update_order(
                order_id,
                {'status': 'completed'}
            )
            
            # 6. Async tasks
            self.executor.submit(
                self._update_analytics,
                customer_id,
                order_id
            )
            
            return {
                'order_id': order_id,
                'status': 'completed',
                'total_amount': order_data['total_amount']
            }
            
        except Exception as e:
            logger.error(f"Order processing error: {e}")
            raise
    
    def get_product_details(
        self,
        product_id: str,
        with_recommendations: bool = True
    ) -> Dict[str, Any]:
        """Get complete product information."""
        try:
            # Get base product data from MongoDB
            product = self.mongo.get_product(product_id)
            if not product:
                raise ValueError(f"Product not found: {product_id}")
            
            # Get real-time stock from Redis
            stock = self.redis.get_stock(product_id)
            product['current_stock'] = stock
            
            # Get price history from TimescaleDB
            price_history = self.timescale.get_price_history(
                product_id,
                days=30
            )
            product['price_history'] = price_history
            
            # Get recommendations if requested
            if with_recommendations:
                similar_products = self.neo4j.get_similar_products(
                    product_id
                )
                product['recommendations'] = similar_products
            
            return product
            
        except Exception as e:
            logger.error(f"Product detail error: {e}")
            raise
    
    def get_customer_insights(
        self,
        customer_id: str
    ) -> Dict[str, Any]:
        """Get comprehensive customer insights."""
        try:
            # Get customer profile from MongoDB
            profile = self.mongo.get_customer_history(customer_id)
            
            # Get current session/cart from Redis
            current_session = self.redis.get_session(customer_id)
            current_cart = self.redis.get_cart(customer_id)
            
            # Get purchase analytics from TimescaleDB
            analytics = self.timescale.get_customer_metrics(
                customer_id,
                days=90
            )
            
            # Get personalized recommendations from Neo4j
            recommendations = self.neo4j.get_recommendations(
                customer_id
            )
            
            return {
                'profile': profile,
                'current_session': current_session,
                'current_cart': current_cart,
                'analytics': analytics,
                'recommendations': recommendations
            }
            
        except Exception as e:
            logger.error(f"Customer insights error: {e}")
            raise
    
    def _update_analytics(
        self,
        customer_id: str,
        order_id: str
    ) -> None:
        """Async analytics updates."""
        try:
            # Update customer metrics
            self.timescale.update_customer_metrics(
                customer_id,
                order_id
            )
            
            # Update recommendation patterns
            self.neo4j.update_purchase_patterns(
                customer_id,
                order_id
            )
            
        except Exception as e:
            logger.error(f"Analytics update error: {e}")
    
    def health_check(self) -> Dict[str, Any]:
        """Check health of all database systems."""
        return {
            'mongo': self.mongo.health_check(),
            'redis': self.redis.health_check(),
            'timescale': self.timescale.health_check(),
            'neo4j': self.neo4j.health_check(),
            'timestamp': datetime.utcnow()
        }
    
    def close(self) -> None:
        """Safely close all database connections."""
        try:
            self.mongo.close()
            self.redis.close()
            self.timescale.close()
            self.neo4j.close()
            self.executor.shutdown()
            logger.info("All database connections closed")
            
        except Exception as e:
            logger.error(f"System shutdown error: {e}")
            raise

def main():
    """Main function."""
    # Configuration for each database
    config = {
        'mongo': {
            'host': 'localhost',
            'port': 27017,
            'database': 'ecommerce'
        },
        'redis': {
            'host': 'localhost',
            'port': 6379,
            'db': 0
        },
        'timescale': {
            'host': 'localhost',
            'port': 5432,
            'database': 'ecommerce',
            'user': 'postgres',
            'password': ''
        },
        'neo4j': {
            'uri': 'bolt://localhost:7687',
            'user': 'neo4j',
            'password': ''
        }
    }
    
    system = EcommerceSystem(config)
    
    try:
        # Example: Process an order
        order = system.process_order(
            'customer123',
            [{
                'product_id': 'prod123',
                'quantity': 2,
                'price': 29.99
            }]
        )
        print(f"Order processed: {order}")
        
        # Example: Get product details
        product = system.get_product_details('prod123')
        print(f"Product details: {product}")
        
        # Example: Get customer insights
        insights = system.get_customer_insights('customer123')
        print(f"Customer insights: {insights}")
        
        # Example: Check system health
        health = system.health_check()
        print(f"System health: {health}")
        
    finally:
        system.close()

if __name__ == '__main__':
    main() 