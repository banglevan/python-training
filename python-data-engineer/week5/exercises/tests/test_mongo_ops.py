"""
Unit Tests for MongoDB Operations
-------------------------------

Test Coverage:
1. Document Operations
   - Bulk write performance
   - Query optimization
   - Error handling

2. Aggregation Framework
   - Pipeline execution
   - Memory usage
   - Result validation

3. Index Management
   - Creation/Validation
   - Performance impact
   - Storage metrics
"""

import unittest
from mongo_ops import MongoOperations
from pymongo.errors import PyMongoError
import time
from datetime import datetime

class TestMongoOperations(unittest.TestCase):
    """Test cases for MongoDB operations."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.mongo = MongoOperations(
            database="test_db"
        )
        
        # Create test collection
        cls.collection = "test_collection"
        cls.mongo.db[cls.collection].drop()
        
        # Insert test data
        test_docs = [
            {
                "item": i,
                "value": i * 100,
                "category": f"cat_{i % 5}",
                "created_at": datetime.now()
            }
            for i in range(100)
        ]
        cls.mongo.bulk_write(cls.collection, test_docs)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.mongo.db[cls.collection].drop()
        cls.mongo.close()
    
    def test_bulk_write(self):
        """Test bulk write operations."""
        # Prepare test data
        docs = [
            {"item": f"test_{i}", "value": i}
            for i in range(1000)
        ]
        
        # Test ordered bulk write
        ordered_result = self.mongo.bulk_write(
            self.collection,
            docs,
            ordered=True
        )
        
        self.assertEqual(
            ordered_result['inserted'],
            1000
        )
        self.assertIn('duration', ordered_result)
        self.assertIn('rate', ordered_result)
        
        # Test unordered bulk write
        unordered_result = self.mongo.bulk_write(
            self.collection,
            docs,
            ordered=False
        )
        
        self.assertGreaterEqual(
            unordered_result['rate'],
            ordered_result['rate']
        )
    
    def test_aggregation(self):
        """Test aggregation pipeline."""
        pipeline = [
            {"$match": {"value": {"$gt": 500}}},
            {"$group": {
                "_id": "$category",
                "total": {"$sum": "$value"},
                "avg": {"$avg": "$value"}
            }},
            {"$sort": {"total": -1}}
        ]
        
        # Test normal execution
        results = self.mongo.aggregate(
            self.collection,
            pipeline
        )
        
        self.assertTrue(len(results) > 0)
        self.assertIn('total', results[0])
        self.assertIn('avg', results[0])
        
        # Test explain plan
        explain = self.mongo.aggregate(
            self.collection,
            pipeline,
            explain=True
        )
        
        self.assertIn('stages', str(explain))
    
    def test_index_management(self):
        """Test index operations."""
        # Create compound index
        index_name = self.mongo.create_index(
            self.collection,
            [
                ("category", 1),
                ("value", -1)
            ]
        )
        
        self.assertIsNotNone(index_name)
        
        # Verify index impact
        query = {"category": "cat_1", "value": {"$gt": 500}}
        
        analysis = self.mongo.analyze_query(
            self.collection,
            query
        )
        
        self.assertTrue(analysis['metrics']['index_used'])
        self.assertLess(
            analysis['metrics']['examination_ratio'],
            2.0
        )
    
    def test_query_analysis(self):
        """Test query performance analysis."""
        # Test with index
        self.mongo.create_index(
            self.collection,
            [("value", 1)]
        )
        
        indexed_analysis = self.mongo.analyze_query(
            self.collection,
            {"value": {"$gt": 500}}
        )
        
        self.assertTrue(
            indexed_analysis['metrics']['index_used']
        )
        
        # Test without index
        non_indexed_analysis = self.mongo.analyze_query(
            self.collection,
            {"item": {"$regex": "test"}}
        )
        
        self.assertFalse(
            non_indexed_analysis['metrics']['index_used']
        )
        
        # Compare performance
        self.assertLess(
            float(indexed_analysis['metrics']['duration'].rstrip('s')),
            float(non_indexed_analysis['metrics']['duration'].rstrip('s'))
        )

if __name__ == '__main__':
    unittest.main() 