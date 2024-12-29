"""
MongoDB Operations & Performance Optimization
------------------------------------------

TARGET:
1. Document Operations
   - Efficient CRUD implementations
   - Bulk write operations
   - Transaction management

2. Aggregation Framework
   - Pipeline optimization
   - Memory constraints
   - Performance monitoring

3. Index Strategies
   - Single/Compound indexes
   - Text/Geospatial indexes
   - Index intersection

IMPLEMENTATION APPROACH:
-----------------------

1. Document Management:
   - Write Operations
     * Ordered vs Unordered bulk writes
     * Write concern levels
     * Journal configuration
   - Read Operations
     * Query optimization
     * Projection strategies
     * Cursor management
   - Update Patterns
     * Atomic operations
     * Field updates
     * Array modifications

2. Aggregation Design:
   - Pipeline Stages
     * $match early filtering
     * $project field selection
     * $group memory usage
   - Performance Factors
     * Document size impact
     * Stage ordering
     * Memory limitations
   - Output Options
     * Cursor results
     * Collection output
     * Explain plans

3. Indexing Strategy:
   - Index Types
     * Single field
     * Compound fields
     * Text search
     * Geospatial
   - Index Properties
     * Unique constraints
     * Sparse options
     * TTL indexes
   - Performance Impact
     * Query patterns
     * Write overhead
     * Storage requirements

PERFORMANCE METRICS:
------------------
1. Operation Metrics
   - Response times
   - Document scanning
   - Index utilization

2. Resource Usage
   - Memory consumption
   - Disk I/O
   - Connection pooling

3. Index Effectiveness
   - Query coverage
   - Write impact
   - Storage overhead

Example Usage:
-------------
mongo = MongoOperations(uri="mongodb://localhost:27017")

# Bulk document operations
docs = [{"item": i, "value": i*100} for i in range(1000)]
result = mongo.bulk_write(docs, ordered=False)

# Aggregation pipeline
pipeline = [
    {"$match": {"status": "active"}},
    {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}}
]
results = mongo.aggregate("sales", pipeline)

# Index management
mongo.create_index("products", [("name", 1), ("category", 1)])
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import PyMongoError
import logging
from typing import Dict, List, Any
from datetime import datetime
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoOperations:
    """MongoDB operations handler."""
    
    def __init__(
        self,
        uri: str = "mongodb://localhost:27017",
        database: str = "test_db"
    ):
        """Initialize MongoDB connection."""
        self.client = MongoClient(uri)
        self.db = self.client[database]
        logger.info(f"Connected to MongoDB: {database}")
    
    def bulk_write(
        self,
        collection: str,
        documents: List[Dict],
        ordered: bool = True
    ) -> Dict:
        """Execute bulk write operations."""
        try:
            start_time = time.time()
            result = self.db[collection].bulk_write([
                {'insertOne': {'document': doc}}
                for doc in documents
            ], ordered=ordered)
            
            duration = time.time() - start_time
            
            metrics = {
                'inserted': result.inserted_count,
                'duration': f"{duration:.2f}s",
                'rate': len(documents) / duration
            }
            
            logger.info(f"Bulk write metrics: {metrics}")
            return metrics
            
        except PyMongoError as e:
            logger.error(f"Bulk write error: {e}")
            raise
    
    def aggregate(
        self,
        collection: str,
        pipeline: List[Dict],
        explain: bool = False
    ) -> List[Dict]:
        """Execute aggregation pipeline."""
        try:
            # Add optimization hints
            if '$match' in str(pipeline):
                self.db[collection].create_index(
                    list(pipeline[0]['$match'].keys())
                )
            
            # Get explain plan if requested
            if explain:
                return self.db[collection].aggregate(
                    pipeline,
                    explain=True
                )
            
            start_time = time.time()
            results = list(
                self.db[collection].aggregate(
                    pipeline,
                    allowDiskUse=True
                )
            )
            duration = time.time() - start_time
            
            metrics = {
                'results': len(results),
                'duration': f"{duration:.2f}s",
                'stages': len(pipeline)
            }
            
            logger.info(f"Aggregation metrics: {metrics}")
            return results
            
        except PyMongoError as e:
            logger.error(f"Aggregation error: {e}")
            raise
    
    def create_index(
        self,
        collection: str,
        keys: List[tuple],
        **options
    ) -> str:
        """Create collection index."""
        try:
            start_time = time.time()
            index_name = self.db[collection].create_index(
                keys,
                **options
            )
            duration = time.time() - start_time
            
            # Get index stats
            stats = self.db[collection].index_information()
            index_size = self.db.command(
                "collStats", 
                collection
            )['indexSizes'].get(index_name, 0)
            
            metrics = {
                'name': index_name,
                'size': f"{index_size / 1024 / 1024:.1f}MB",
                'duration': f"{duration:.2f}s"
            }
            
            logger.info(f"Index creation metrics: {metrics}")
            return index_name
            
        except PyMongoError as e:
            logger.error(f"Index creation error: {e}")
            raise
    
    def analyze_query(
        self,
        collection: str,
        query: Dict,
        projection: Dict = None
    ) -> Dict:
        """Analyze query performance."""
        try:
            # Get explain plan
            plan = self.db[collection].find(
                query,
                projection
            ).explain()
            
            # Execute query with timing
            start_time = time.time()
            results = list(
                self.db[collection].find(
                    query,
                    projection
                )
            )
            duration = time.time() - start_time
            
            # Analyze index usage
            index_used = 'IXSCAN' in str(plan)
            docs_examined = plan['executionStats']['totalDocsExamined']
            docs_returned = plan['executionStats']['nReturned']
            
            metrics = {
                'duration': f"{duration:.2f}s",
                'result_count': len(results),
                'index_used': index_used,
                'docs_examined': docs_examined,
                'docs_returned': docs_returned,
                'examination_ratio': docs_examined / max(docs_returned, 1)
            }
            
            logger.info(f"Query metrics: {metrics}")
            return {
                'metrics': metrics,
                'plan': plan
            }
            
        except PyMongoError as e:
            logger.error(f"Query analysis error: {e}")
            raise
    
    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

def main():
    """Main function."""
    mongo = MongoOperations()
    
    try:
        # Example: Bulk write
        docs = [
            {"item": i, "value": i * 100}
            for i in range(1000)
        ]
        mongo.bulk_write("test_collection", docs)
        
        # Example: Aggregation
        pipeline = [
            {"$match": {"value": {"$gt": 500}}},
            {"$group": {
                "_id": None,
                "total": {"$sum": "$value"},
                "avg": {"$avg": "$value"}
            }}
        ]
        results = mongo.aggregate(
            "test_collection",
            pipeline
        )
        
        # Example: Index creation
        mongo.create_index(
            "test_collection",
            [("item", ASCENDING)],
            unique=True
        )
        
        # Example: Query analysis
        analysis = mongo.analyze_query(
            "test_collection",
            {"value": {"$gt": 500}}
        )
        
    finally:
        mongo.close()

if __name__ == '__main__':
    main() 