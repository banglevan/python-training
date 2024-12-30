"""
Unit Tests for Redis Caching
---------------------------

Test Coverage:
1. Data Structure Operations
   - String operations
   - Hash operations
   - List operations

2. Caching Patterns
   - TTL management
   - Cache loading
   - Hit/Miss rates

3. Performance Metrics
   - Operation timing
   - Memory usage
   - Error handling
"""

import unittest
from redis_cache import RedisCache
import time
from datetime import datetime
import json

class TestRedisCache(unittest.TestCase):
    """Test cases for Redis caching."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.cache = RedisCache(db=15)  # Use separate DB for testing
        cls.cache.redis.flushdb()  # Clear test database
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.cache.redis.flushdb()
        cls.cache.close()
    
    def test_set_with_ttl(self):
        """Test TTL-based caching."""
        # Test normal set
        result = self.cache.set_with_ttl(
            "test:key",
            {"data": "value"},
            ttl=10
        )
        
        self.assertTrue(result)
        
        # Verify TTL
        ttl = self.cache.redis.ttl("test:key")
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 10)
        
        # Test expiration
        self.cache.set_with_ttl(
            "expire:key",
            {"data": "temp"},
            ttl=1
        )
        
        time.sleep(2)
        value = self.cache.redis.get("expire:key")
        self.assertIsNone(value)
    
    def test_get_or_load(self):
        """Test cache loading pattern."""
        def load_data():
            return {"fresh": "data"}
        
        # Test cache miss
        miss_start = time.time()
        value = self.cache.get_or_load(
            "missing:key",
            load_data,
            ttl=30
        )
        miss_duration = time.time() - miss_start
        
        self.assertEqual(value["fresh"], "data")
        
        # Test cache hit
        hit_start = time.time()
        cached_value = self.cache.get_or_load(
            "missing:key",
            load_data,
            ttl=30
        )
        hit_duration = time.time() - hit_start
        
        self.assertEqual(cached_value, value)
        self.assertLess(hit_duration, miss_duration)
    
    def test_hash_operations(self):
        """Test hash structure operations."""
        # Test hash set
        mapping = {
            "field1": "value1",
            "field2": 123,
            "field3": {"nested": "data"}
        }
        
        result = self.cache.hash_set(
            "test:hash",
            mapping,
            ttl=60
        )
        
        self.assertTrue(result)
        
        # Verify fields
        for field, expected in mapping.items():
            value = self.cache.redis.hget("test:hash", field)
            self.assertEqual(
                json.loads(value),
                expected
            )
    
    def test_list_operations(self):
        """Test list structure operations."""
        # Test push operation
        items = [
            {"id": 1, "data": "first"},
            {"id": 2, "data": "second"}
        ]
        
        for item in items:
            self.cache.list_operations(
                "test:list",
                "push",
                item
            )
        
        # Test pop operation
        popped = self.cache.list_operations(
            "test:list",
            "pop",
            timeout=1
        )
        
        self.assertEqual(popped["id"], 2)
        
        # Test timeout
        empty = self.cache.list_operations(
            "empty:list",
            "pop",
            timeout=1
        )
        
        self.assertIsNone(empty)
    
    def test_monitoring(self):
        """Test statistics monitoring."""
        # Generate some activity
        for i in range(100):
            self.cache.set_with_ttl(
                f"test:key:{i}",
                {"data": i},
                ttl=30
            )
        
        stats = self.cache.monitor_stats()
        
        # Verify metrics
        self.assertIn('memory', stats)
        self.assertIn('stats', stats)
        self.assertIn('persistence', stats)
        
        # Check hit rate
        self.assertGreaterEqual(
            stats['stats']['hit_rate'],
            0.0
        )
        self.assertLessEqual(
            stats['stats']['hit_rate'],
            1.0
        )
        
        # Verify memory metrics
        self.assertGreater(
            float(stats['memory']['used'].rstrip('MB')),
            0
        )

if __name__ == '__main__':
    unittest.main() 