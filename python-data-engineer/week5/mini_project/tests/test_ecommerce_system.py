"""
E-commerce System Tests
---------------------

PURPOSE:
Test the integration and functionality of:
1. System Operations
   - Order processing
   - Product management
   - Customer analytics

2. Data Consistency
   - Cross-database sync
   - Transaction integrity
   - Error handling

3. Performance
   - Response times
   - Resource usage
   - Concurrency
"""

import unittest
from unittest.mock import Mock, patch
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import logging

from ecommerce_system import EcommerceSystem

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestEcommerceSystem(unittest.TestCase):
    """Test cases for E-commerce System."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.config = {
            'mongo': {
                'host': 'localhost',
                'port': 27017,
                'database': 'ecommerce_test'
            },
            'redis': {
                'host': 'localhost',
                'port': 6379,
                'db': 1
            },
            'timescale': {
                'host': 'localhost',
                'port': 5432,
                'database': 'ecommerce_test',
                'user': 'postgres',
                'password': ''
            },
            'neo4j': {
                'uri': 'bolt://localhost:7687',
                'user': 'neo4j',
                'password': 'test'
            }
        }
    
    def setUp(self):
        """Initialize system for each test."""
        # Create mock database managers
        self.mock_mongo = Mock()
        self.mock_redis = Mock()
        self.mock_timescale = Mock()
        self.mock_neo4j = Mock()
        
        # Patch database managers
        self.patches = [
            patch('ecommerce_system.EcommerceMongoManager',
                  return_value=self.mock_mongo),
            patch('ecommerce_system.EcommerceRedisManager',
                  return_value=self.mock_redis),
            patch('ecommerce_system.EcommerceTimescaleManager',
                  return_value=self.mock_timescale),
            patch('ecommerce_system.EcommerceNeo4jManager',
                  return_value=self.mock_neo4j)
        ]
        
        # Start patches
        for p in self.patches:
            p.start()
        
        # Initialize system
        self.system = EcommerceSystem(self.config)
    
    def tearDown(self):
        """Clean up after each test."""
        # Stop patches
        for p in self.patches:
            p.stop()
        
        # Close system
        self.system.close()
    
    def test_order_processing(self):
        """Test complete order processing flow."""
        # Setup test data
        customer_id = 'test_customer'
        items = [{
            'product_id': 'test_product',
            'quantity': 2,
            'price': 29.99
        }]
        
        # Mock responses
        self.mock_redis.get_cart.return_value = items
        self.mock_redis.check_stock.return_value = True
        self.mock_mongo.add_order.return_value = 'test_order'
        
        # Process order
        result = self.system.process_order(customer_id, items)
        
        # Verify results
        self.assertEqual(result['order_id'], 'test_order')
        self.assertEqual(result['status'], 'completed')
        self.assertAlmostEqual(
            result['total_amount'],
            59.98
        )
        
        # Verify interactions
        self.mock_redis.check_stock.assert_called_once()
        self.mock_mongo.add_order.assert_called_once()
        self.mock_redis.update_stock.assert_called_once()
        self.mock_timescale.record_sale.assert_called_once()
        self.mock_neo4j.record_purchase.assert_called_once()
    
    def test_product_details(self):
        """Test product details retrieval."""
        product_id = 'test_product'
        
        # Mock responses
        self.mock_mongo.get_product.return_value = {
            'product_id': product_id,
            'name': 'Test Product',
            'price': 29.99
        }
        self.mock_redis.get_stock.return_value = 100
        self.mock_timescale.get_price_history.return_value = [
            {'date': '2024-01-01', 'price': 29.99}
        ]
        self.mock_neo4j.get_similar_products.return_value = [
            {'product_id': 'similar1', 'score': 0.8}
        ]
        
        # Get product details
        result = self.system.get_product_details(
            product_id,
            with_recommendations=True
        )
        
        # Verify results
        self.assertEqual(result['product_id'], product_id)
        self.assertEqual(result['current_stock'], 100)
        self.assertTrue('price_history' in result)
        self.assertTrue('recommendations' in result)
        
        # Verify interactions
        self.mock_mongo.get_product.assert_called_once()
        self.mock_redis.get_stock.assert_called_once()
        self.mock_timescale.get_price_history.assert_called_once()
        self.mock_neo4j.get_similar_products.assert_called_once()
    
    def test_customer_insights(self):
        """Test customer insights retrieval."""
        customer_id = 'test_customer'
        
        # Mock responses
        self.mock_mongo.get_customer_history.return_value = {
            'total_orders': 5,
            'total_spent': 299.95
        }
        self.mock_redis.get_session.return_value = {
            'last_active': datetime.utcnow().isoformat()
        }
        self.mock_redis.get_cart.return_value = [
            {'product_id': 'test_product', 'quantity': 1}
        ]
        self.mock_timescale.get_customer_metrics.return_value = {
            'purchase_frequency': 'weekly',
            'avg_order_value': 59.99
        }
        self.mock_neo4j.get_recommendations.return_value = [
            {'product_id': 'rec1', 'score': 0.9}
        ]
        
        # Get customer insights
        result = self.system.get_customer_insights(customer_id)
        
        # Verify results
        self.assertTrue('profile' in result)
        self.assertTrue('current_session' in result)
        self.assertTrue('current_cart' in result)
        self.assertTrue('analytics' in result)
        self.assertTrue('recommendations' in result)
        
        # Verify interactions
        self.mock_mongo.get_customer_history.assert_called_once()
        self.mock_redis.get_session.assert_called_once()
        self.mock_redis.get_cart.assert_called_once()
        self.mock_timescale.get_customer_metrics.assert_called_once()
        self.mock_neo4j.get_recommendations.assert_called_once()
    
    def test_error_handling(self):
        """Test system error handling."""
        # Test insufficient stock
        self.mock_redis.check_stock.return_value = False
        
        with self.assertRaises(ValueError):
            self.system.process_order(
                'test_customer',
                [{
                    'product_id': 'test_product',
                    'quantity': 999,
                    'price': 29.99
                }]
            )
        
        # Test product not found
        self.mock_mongo.get_product.return_value = None
        
        with self.assertRaises(ValueError):
            self.system.get_product_details('nonexistent')
    
    def test_concurrent_operations(self):
        """Test system under concurrent operations."""
        def process_order(i):
            return self.system.process_order(
                f'customer_{i}',
                [{
                    'product_id': f'product_{i}',
                    'quantity': 1,
                    'price': 29.99
                }]
            )
        
        # Setup mocks for concurrent operations
        self.mock_redis.check_stock.return_value = True
        self.mock_mongo.add_order.side_effect = lambda x: f'order_{x["customer_id"]}'
        
        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(
                process_order,
                range(10)
            ))
        
        # Verify results
        self.assertEqual(len(results), 10)
        for i, result in enumerate(results):
            self.assertEqual(
                result['order_id'],
                f'order_customer_{i}'
            )
    
    def test_health_check(self):
        """Test system health check."""
        # Mock health responses
        health_response = {
            'status': 'healthy',
            'timestamp': datetime.utcnow()
        }
        
        self.mock_mongo.health_check.return_value = health_response
        self.mock_redis.health_check.return_value = health_response
        self.mock_timescale.health_check.return_value = health_response
        self.mock_neo4j.health_check.return_value = health_response
        
        # Check system health
        result = self.system.health_check()
        
        # Verify results
        self.assertTrue('mongo' in result)
        self.assertTrue('redis' in result)
        self.assertTrue('timescale' in result)
        self.assertTrue('neo4j' in result)
        self.assertTrue('timestamp' in result)
        
        # Verify all services are healthy
        for service in ['mongo', 'redis', 'timescale', 'neo4j']:
            self.assertEqual(
                result[service]['status'],
                'healthy'
            )

if __name__ == '__main__':
    unittest.main() 