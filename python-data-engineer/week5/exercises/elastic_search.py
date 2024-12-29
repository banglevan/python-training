"""
Elasticsearch Search & Analytics
------------------------------

TARGET:
1. Index Management
   - Mapping optimization
   - Shard strategy
   - Refresh control

2. Search Operations
   - Query DSL mastery
   - Relevance tuning
   - Performance optimization

3. Aggregation Framework
   - Metrics calculation
   - Bucket analysis
   - Pipeline aggregations

IMPLEMENTATION APPROACH:
-----------------------

1. Index Design:
   - Mapping Strategy
     * Field types
     * Dynamic templates
     * Multi-fields
   - Settings Configuration
     * Shard allocation
     * Replica count
     * Refresh interval
   - Lifecycle Management
     * Rollover policy
     * Retention rules
     * Backup strategy

2. Search Optimization:
   - Query Types
     * Match/Term queries
     * Bool compounds
     * Function score
   - Filtering Techniques
     * Filter context
     * Caching
     * Cost reduction
   - Relevance Control
     * Boosting factors
     * Field weights
     * Decay functions

3. Aggregation Design:
   - Metric Aggregations
     * Statistical analysis
     * Cardinality estimation
     * Percentile calculation
   - Bucket Aggregations
     * Terms grouping
     * Date histograms
     * Range analysis
   - Pipeline Processing
     * Moving averages
     * Cumulative sums
     * Bucket scripts

PERFORMANCE METRICS:
------------------
1. Search Performance
   - Response time
   - Request rate
   - Shard usage

2. Index Efficiency
   - Indexing throughput
   - Refresh impact
   - Merge overhead

3. Resource Usage
   - Memory allocation
   - CPU utilization
   - Disk I/O

Example Usage:
-------------
es = ElasticSearch(['localhost:9200'])

# Create optimized index
es.create_index(
    'products',
    mappings={
        'name': {'type': 'text', 'analyzer': 'english'},
        'price': {'type': 'double'},
        'category': {'type': 'keyword'}
    }
)

# Complex search with aggregations
results = es.search(
    'products',
    query={'match': {'name': 'laptop'}},
    aggs={'avg_price': {'avg': {'field': 'price'}}}
)
"""

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ElasticsearchException
import logging
from typing import Dict, List, Any
from datetime import datetime
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ElasticSearch:
    """Elasticsearch operations handler."""
    
    def __init__(
        self,
        hosts: List[str] = ['localhost:9200'],
        **kwargs
    ):
        """Initialize Elasticsearch connection."""
        self.es = Elasticsearch(hosts, **kwargs)
        logger.info(f"Connected to Elasticsearch: {hosts}")
    
    def create_index(
        self,
        index: str,
        mappings: Dict = None,
        settings: Dict = None
    ):
        """Create index with optimized configuration."""
        try:
            # Default settings
            if settings is None:
                settings = {
                    'number_of_shards': 3,
                    'number_of_replicas': 1,
                    'refresh_interval': '1s'
                }
            
            # Create index
            self.es.indices.create(
                index=index,
                body={
                    'settings': settings,
                    'mappings': {
                        'properties': mappings
                    } if mappings else {}
                }
            )
            
            logger.info(f"Created index: {index}")
            
        except ElasticsearchException as e:
            logger.error(f"Index creation error: {e}")
            raise
    
    def bulk_index(
        self,
        index: str,
        documents: List[Dict],
        chunk_size: int = 500
    ):
        """Bulk index documents with optimization."""
        try:
            start_time = time.time()
            
            # Prepare actions
            actions = [
                {
                    '_index': index,
                    '_source': doc
                }
                for doc in documents
            ]
            
            # Execute bulk indexing
            success, failed = helpers.bulk(
                self.es,
                actions,
                chunk_size=chunk_size,
                stats_only=True
            )
            
            duration = time.time() - start_time
            
            metrics = {
                'operation': 'bulk_index',
                'index': index,
                'success': success,
                'failed': failed,
                'rate': len(documents) / duration,
                'duration': f"{duration:.2f}s"
            }
            
            logger.info(f"Bulk index metrics: {metrics}")
            return metrics
            
        except ElasticsearchException as e:
            logger.error(f"Bulk index error: {e}")
            raise
    
    def search(
        self,
        index: str,
        query: Dict = None,
        aggs: Dict = None,
        source: List[str] = None,
        size: int = 10,
        **kwargs
    ):
        """Execute search with aggregations."""
        try:
            start_time = time.time()
            
            # Build search body
            body = {}
            
            if query:
                body['query'] = query
            
            if aggs:
                body['aggs'] = aggs
            
            # Execute search
            response = self.es.search(
                index=index,
                body=body,
                _source=source,
                size=size,
                **kwargs
            )
            
            duration = time.time() - start_time
            
            metrics = {
                'operation': 'search',
                'index': index,
                'hits': response['hits']['total']['value'],
                'took': response['took'],
                'duration': f"{duration:.6f}s"
            }
            
            logger.info(f"Search metrics: {metrics}")
            return response
            
        except ElasticsearchException as e:
            logger.error(f"Search error: {e}")
            raise
    
    def update_settings(
        self,
        index: str,
        settings: Dict
    ):
        """Update index settings."""
        try:
            self.es.indices.put_settings(
                index=index,
                body=settings
            )
            
            logger.info(
                f"Updated settings for index: {index}"
            )
            
        except ElasticsearchException as e:
            logger.error(f"Settings update error: {e}")
            raise
    
    def analyze_query(
        self,
        index: str,
        query: Dict
    ):
        """Analyze query performance."""
        try:
            # Get query explanation
            explanation = self.es.indices.validate_query(
                index=index,
                body={'query': query},
                explain=True
            )
            
            # Profile query execution
            profile = self.es.search(
                index=index,
                body={'query': query},
                profile=True
            )
            
            analysis = {
                'valid': explanation['valid'],
                'explanation': explanation.get(
                    'explanations', []
                ),
                'profile': profile['profile'],
                'shards': profile['_shards']
            }
            
            logger.info(f"Query analysis: {analysis}")
            return analysis
            
        except ElasticsearchException as e:
            logger.error(f"Query analysis error: {e}")
            raise
    
    def monitor_stats(
        self,
        metrics: List[str] = None
    ):
        """Monitor cluster and index statistics."""
        try:
            stats = {
                'cluster': self.es.cluster.stats(),
                'nodes': self.es.nodes.stats(),
                'indices': self.es.indices.stats()
            }
            
            if metrics:
                filtered_stats = {}
                for category, data in stats.items():
                    filtered_stats[category] = {
                        k: v for k, v in data.items()
                        if k in metrics
                    }
                stats = filtered_stats
            
            logger.info("Collected cluster statistics")
            return stats
            
        except ElasticsearchException as e:
            logger.error(f"Stats monitoring error: {e}")
            raise
    
    def close(self):
        """Close Elasticsearch connection."""
        if self.es:
            self.es.close()
            logger.info("Elasticsearch connection closed")

def main():
    """Main function."""
    es = ElasticSearch()
    
    try:
        # Example: Create index
        es.create_index(
            'products',
            mappings={
                'name': {
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
            },
            settings={
                'number_of_shards': 3,
                'number_of_replicas': 1,
                'refresh_interval': '1s'
            }
        )
        
        # Example: Bulk index
        products = [
            {
                'name': f'Product {i}',
                'price': i * 10.0,
                'category': f'cat_{i % 5}',
                'created_at': datetime.now().isoformat()
            }
            for i in range(1000)
        ]
        
        es.bulk_index('products', products)
        
        # Example: Complex search
        response = es.search(
            'products',
            query={
                'bool': {
                    'must': [
                        {'match': {'name': 'Product'}},
                        {'range': {'price': {'gte': 100}}}
                    ],
                    'filter': [
                        {'term': {'category': 'cat_1'}}
                    ]
                }
            },
            aggs={
                'avg_price': {'avg': {'field': 'price'}},
                'categories': {
                    'terms': {'field': 'category'},
                    'aggs': {
                        'avg_price': {'avg': {'field': 'price'}}
                    }
                }
            }
        )
        
        # Example: Analyze query
        analysis = es.analyze_query(
            'products',
            {
                'match': {
                    'name': 'Product'
                }
            }
        )
        
        # Example: Monitor stats
        stats = es.monitor_stats([
            'docs',
            'store',
            'indexing',
            'search'
        ])
        
    finally:
        es.close()

if __name__ == '__main__':
    main() 