"""
Unit Tests for Elasticsearch Operations
-----------------------------------

Test Coverage:
1. Index Management
   - Creation/Deletion
   - Mapping updates
   - Settings configuration

2. Search Operations
   - Query execution
   - Aggregation results
   - Relevance scoring

3. Performance Analysis
   - Query profiling
   - Bulk operations
   - Statistics monitoring
"""

import unittest
from elastic_search import ElasticSearch
from elasticsearch import ElasticsearchException
from datetime import datetime
import time

class TestElasticSearch(unittest.TestCase):
    """Test cases for Elasticsearch operations."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.es = ElasticSearch(['localhost:9200'])
        cls.index = 'test_index'
        
        # Create test index
        cls.es.create_index(
            cls.index,
            mappings={
                'title': {
                    'type': 'text',
                    'analyzer': 'english',
                    'fields': {
                        'keyword': {
                            'type': 'keyword'
                        }
                    }
                },
                'price': {'type': 'double'},
                'category': {'type': 'keyword'},
                'created_at': {'type': 'date'}
            }
        )
        
        # Insert test data
        docs = [
            {
                'title': f'Test Product {i}',
                'price': i * 10.0,
                'category': f'cat_{i % 5}',
                'created_at': datetime.now().isoformat()
            }
            for i in range(100)
        ]
        
        cls.es.bulk_index(cls.index, docs)
        
        # Ensure index refresh
        cls.es.es.indices.refresh(index=cls.index)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.es.es.indices.delete(
            index=cls.index,
            ignore=[404]
        )
        cls.es.close()
    
    def test_index_creation(self):
        """Test index creation and mapping."""
        # Create new index
        new_index = 'test_creation'
        
        self.es.create_index(
            new_index,
            mappings={
                'field1': {'type': 'text'},
                'field2': {'type': 'keyword'},
                'field3': {
                    'type': 'nested',
                    'properties': {
                        'nested1': {'type': 'text'},
                        'nested2': {'type': 'long'}
                    }
                }
            },
            settings={
                'number_of_shards': 2,
                'number_of_replicas': 0
            }
        )
        
        # Verify mapping
        mapping = self.es.es.indices.get_mapping(
            index=new_index
        )
        
        self.assertIn(new_index, mapping)
        self.assertIn(
            'field3',
            mapping[new_index]['mappings']['properties']
        )
        
        # Clean up
        self.es.es.indices.delete(index=new_index)
    
    def test_search_operations(self):
        """Test search functionality."""
        # Test match query
        response = self.es.search(
            self.index,
            query={
                'match': {
                    'title': 'Test Product'
                }
            }
        )
        
        self.assertGreater(
            response['hits']['total']['value'],
            0
        )
        
        # Test bool query with filter
        response = self.es.search(
            self.index,
            query={
                'bool': {
                    'must': [
                        {'match': {'title': 'Product'}}
                    ],
                    'filter': [
                        {'term': {'category': 'cat_1'}},
                        {'range': {'price': {'gte': 50}}}
                    ]
                }
            }
        )
        
        hits = response['hits']['hits']
        self.assertTrue(
            all(h['_source']['category'] == 'cat_1'
                for h in hits)
        )
        self.assertTrue(
            all(h['_source']['price'] >= 50
                for h in hits)
        )
    
    def test_aggregations(self):
        """Test aggregation framework."""
        response = self.es.search(
            self.index,
            size=0,
            aggs={
                'avg_price': {
                    'avg': {'field': 'price'}
                },
                'price_ranges': {
                    'range': {
                        'field': 'price',
                        'ranges': [
                            {'to': 50},
                            {'from': 50, 'to': 100},
                            {'from': 100}
                        ]
                    }
                },
                'categories': {
                    'terms': {
                        'field': 'category',
                        'size': 10
                    },
                    'aggs': {
                        'avg_price': {
                            'avg': {'field': 'price'}
                        }
                    }
                }
            }
        )
        
        aggs = response['aggregations']
        
        self.assertIn('avg_price', aggs)
        self.assertIn('price_ranges', aggs)
        self.assertIn('categories', aggs)
        
        self.assertEqual(
            len(aggs['categories']['buckets']),
            5
        )
    
    def test_bulk_operations(self):
        """Test bulk indexing performance."""
        docs = [
            {
                'title': f'Bulk Test {i}',
                'price': i * 5.0,
                'category': 'bulk_test',
                'created_at': datetime.now().isoformat()
            }
            for i in range(1000)
        ]
        
        # Test different chunk sizes
        chunk_sizes = [100, 500]
        metrics = {}
        
        for size in chunk_sizes:
            metrics[size] = self.es.bulk_index(
                self.index,
                docs,
                chunk_size=size
            )
        
        # Compare performance
        self.assertGreater(
            metrics[500]['rate'],
            metrics[100]['rate']
        )
    
    def test_query_analysis(self):
        """Test query analysis and profiling."""
        query = {
            'bool': {
                'must': [
                    {'match': {'title': 'Product'}}
                ],
                'filter': [
                    {'range': {'price': {'gte': 50}}}
                ]
            }
        }
        
        analysis = self.es.analyze_query(
            self.index,
            query
        )
        
        self.assertTrue(analysis['valid'])
        self.assertIn('profile', analysis)
        self.assertIn('shards', analysis)

if __name__ == '__main__':
    unittest.main() 