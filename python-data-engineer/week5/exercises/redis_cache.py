"""
Redis Caching Strategies & Patterns
---------------------------------

TARGET:
1. Data Structure Selection
   - Optimal type choice
   - Memory efficiency
   - Access patterns

2. Caching Patterns
   - Cache-aside
   - Write-through
   - Write-behind

3. Persistence Configuration
   - RDB snapshots
   - AOF logging
   - Hybrid persistence

IMPLEMENTATION APPROACH:
-----------------------

1. Data Structures:
   - String Operations
     * Simple key-value
     * Bit operations
     * Atomic counters
   - List Management
     * Queue implementations
     * Stack operations
     * Blocking ops
   - Hash Tables
     * Field mapping
     * Partial updates
     * Memory optimization
   - Sets & Sorted Sets
     * Member management
     * Score-based ops
     * Range queries

2. Caching Strategies:
   - Cache Loading
     * Lazy loading
     * Eager loading
     * Refresh ahead
   - Write Policies
     * Write-through
     * Write-behind
     * Write-around
   - Eviction Policies
     * LRU/LFU
     * TTL management
     * Memory limits

3. Persistence Options:
   - RDB Configuration
     * Save frequency
     * Child process
     * Compression
   - AOF Settings
     * Sync options
     * Rewrite threshold
     * Recovery time
   - Hybrid Mode
     * Combined benefits
     * Trade-offs
     * Recovery strategy

PERFORMANCE CONSIDERATIONS:
-------------------------
1. Memory Management
   - Working set size
   - Eviction policies
   - Fragmentation

2. Network Impact
   - Latency
   - Bandwidth
   - Connection pooling

3. Persistence Overhead
   - Disk I/O
   - CPU usage
   - Recovery time

Example Usage:
-------------
cache = RedisCache(host="localhost", port=6379)

# String operations
cache.set_with_ttl("user:123", user_data, ttl=3600)
user = cache.get_or_load("user:123", load_user_from_db)

# List operations
cache.lpush("queue:tasks", task_data)
task = cache.brpop("queue:tasks", timeout=5)

# Hash operations
cache.hset("product:456", {"price": 99.99, "stock": 100})
"""

import redis
from redis.exceptions import RedisError
import logging
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
import json
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisCache:
    """Redis caching handler."""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0
    ):
        """Initialize Redis connection."""
        self.redis = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True
        )
        logger.info(f"Connected to Redis: {host}:{port}")
    
    def set_with_ttl(
        self,
        key: str,
        value: Any,
        ttl: int = 3600
    ) -> bool:
        """Set key with TTL."""
        try:
            start_time = time.time()
            result = self.redis.setex(
                key,
                ttl,
                json.dumps(value)
            )
            duration = time.time() - start_time
            
            metrics = {
                'operation': 'set',
                'key': key,
                'ttl': ttl,
                'duration': f"{duration:.6f}s"
            }
            
            logger.info(f"Cache set metrics: {metrics}")
            return result
            
        except RedisError as e:
            logger.error(f"Cache set error: {e}")
            raise
    
    def get_or_load(
        self,
        key: str,
        loader: Callable,
        ttl: int = 3600
    ) -> Any:
        """Get from cache or load from source."""
        try:
            # Try cache first
            start_time = time.time()
            cached = self.redis.get(key)
            
            if cached:
                value = json.loads(cached)
                duration = time.time() - start_time
                
                metrics = {
                    'operation': 'get',
                    'key': key,
                    'cache_hit': True,
                    'duration': f"{duration:.6f}s"
                }
                
                logger.info(f"Cache get metrics: {metrics}")
                return value
            
            # Load from source
            value = loader()
            
            # Cache the loaded value
            self.set_with_ttl(key, value, ttl)
            
            duration = time.time() - start_time
            metrics = {
                'operation': 'load',
                'key': key,
                'cache_hit': False,
                'duration': f"{duration:.6f}s"
            }
            
            logger.info(f"Cache load metrics: {metrics}")
            return value
            
        except RedisError as e:
            logger.error(f"Cache get/load error: {e}")
            raise
    
    def hash_set(
        self,
        key: str,
        mapping: Dict,
        ttl: Optional[int] = None
    ) -> bool:
        """Set hash fields."""
        try:
            pipeline = self.redis.pipeline()
            
            # Set hash fields
            pipeline.hset(
                key,
                mapping={
                    k: json.dumps(v)
                    for k, v in mapping.items()
                }
            )
            
            # Set TTL if specified
            if ttl:
                pipeline.expire(key, ttl)
            
            start_time = time.time()
            result = pipeline.execute()
            duration = time.time() - start_time
            
            metrics = {
                'operation': 'hset',
                'key': key,
                'fields': len(mapping),
                'duration': f"{duration:.6f}s"
            }
            
            logger.info(f"Hash set metrics: {metrics}")
            return all(result)
            
        except RedisError as e:
            logger.error(f"Hash set error: {e}")
            raise
    
    def list_operations(
        self,
        key: str,
        operation: str,
        *args,
        **kwargs
    ) -> Any:
        """Execute list operations."""
        try:
            start_time = time.time()
            
            if operation == 'push':
                result = self.redis.lpush(
                    key,
                    *[json.dumps(arg) for arg in args]
                )
            elif operation == 'pop':
                result = self.redis.brpop(
                    key,
                    kwargs.get('timeout', 0)
                )
                if result:
                    result = json.loads(result[1])
            else:
                raise ValueError(f"Invalid operation: {operation}")
            
            duration = time.time() - start_time
            metrics = {
                'operation': f"list_{operation}",
                'key': key,
                'duration': f"{duration:.6f}s"
            }
            
            logger.info(f"List operation metrics: {metrics}")
            return result
            
        except RedisError as e:
            logger.error(f"List operation error: {e}")
            raise
    
    def monitor_stats(self) -> Dict:
        """Monitor Redis statistics."""
        try:
            info = self.redis.info()
            
            metrics = {
                'memory': {
                    'used': f"{info['used_memory'] / 1024 / 1024:.1f}MB",
                    'peak': f"{info['used_memory_peak'] / 1024 / 1024:.1f}MB",
                    'fragmentation': info['mem_fragmentation_ratio']
                },
                'stats': {
                    'connected_clients': info['connected_clients'],
                    'total_commands': info['total_commands_processed'],
                    'hits': info['keyspace_hits'],
                    'misses': info['keyspace_misses'],
                    'hit_rate': (
                        info['keyspace_hits'] /
                        max(
                            info['keyspace_hits'] +
                            info['keyspace_misses'],
                            1
                        )
                    )
                },
                'persistence': {
                    'rdb_changes': info['rdb_changes_since_last_save'],
                    'last_save': datetime.fromtimestamp(
                        info['rdb_last_save_time']
                    )
                }
            }
            
            logger.info(f"Redis stats: {metrics}")
            return metrics
            
        except RedisError as e:
            logger.error(f"Stats monitoring error: {e}")
            raise
    
    def close(self):
        """Close Redis connection."""
        if self.redis:
            self.redis.close()
            logger.info("Redis connection closed")

def main():
    """Main function."""
    cache = RedisCache()
    
    try:
        # Example: Cache with TTL
        cache.set_with_ttl(
            "user:123",
            {"name": "Test User", "age": 30},
            ttl=3600
        )
        
        # Example: Get or load
        def load_user():
            return {"name": "New User", "age": 25}
        
        user = cache.get_or_load(
            "user:456",
            load_user,
            ttl=3600
        )
        
        # Example: Hash operations
        cache.hash_set(
            "product:789",
            {
                "name": "Test Product",
                "price": 99.99,
                "stock": 100
            },
            ttl=7200
        )
        
        # Example: List operations
        cache.list_operations(
            "queue:tasks",
            "push",
            {"task_id": 1, "status": "pending"}
        )
        
        task = cache.list_operations(
            "queue:tasks",
            "pop",
            timeout=5
        )
        
        # Example: Monitor stats
        stats = cache.monitor_stats()
        
    finally:
        cache.close()

if __name__ == '__main__':
    main() 