"""
E-commerce Neo4j Manager
----------------------

PURPOSE:
Handles all graph-based operations:
1. Product Recommendations
   - Similar products
   - Frequently bought together
   - Category relationships

2. Customer Behavior
   - Purchase patterns
   - Product browsing paths
   - Category preferences

3. Market Basket Analysis
   - Product associations
   - Bundle suggestions
   - Cross-selling opportunities

GRAPH STRUCTURE:
1. Nodes
   - Products
   - Customers
   - Categories
   - Orders

2. Relationships
   - PURCHASED
   - VIEWED
   - BELONGS_TO
   - SIMILAR_TO
"""

from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase
import logging
from datetime import datetime
from .base_manager import BaseManager

logger = logging.getLogger(__name__)

class EcommerceNeo4jManager(BaseManager):
    """Neo4j manager for e-commerce recommendations."""
    
    def _connect(self) -> None:
        """Initialize Neo4j connection."""
        try:
            self.driver = GraphDatabase.driver(
                self.config.get('uri', 'bolt://localhost:7687'),
                auth=(
                    self.config.get('user', 'neo4j'),
                    self.config.get('password', '')
                )
            )
            
            # Initialize graph schema
            self._setup_schema()
            logger.info("E-commerce Neo4j initialized")
            
        except Exception as e:
            logger.error(f"Neo4j initialization error: {e}")
            raise
    
    def _setup_schema(self) -> None:
        """Setup graph schema and constraints."""
        try:
            with self.driver.session() as session:
                # Create constraints
                session.run("""
                    CREATE CONSTRAINT product_id IF NOT EXISTS
                    FOR (p:Product) REQUIRE p.product_id IS UNIQUE
                """)
                
                session.run("""
                    CREATE CONSTRAINT customer_id IF NOT EXISTS
                    FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE
                """)
                
                session.run("""
                    CREATE CONSTRAINT category_id IF NOT EXISTS
                    FOR (cat:Category) REQUIRE cat.category_id IS UNIQUE
                """)
                
                # Create indexes
                session.run("""
                    CREATE INDEX product_name IF NOT EXISTS
                    FOR (p:Product) ON (p.name)
                """)
                
                session.run("""
                    CREATE INDEX category_name IF NOT EXISTS
                    FOR (cat:Category) ON (cat.name)
                """)
                
        except Exception as e:
            logger.error(f"Schema setup error: {e}")
            raise
    
    def record_purchase(
        self,
        customer_id: str,
        product_id: str,
        order_data: Dict[str, Any]
    ) -> None:
        """Record a purchase relationship."""
        try:
            with self.driver.session() as session:
                session.run("""
                    MERGE (c:Customer {customer_id: $customer_id})
                    MERGE (p:Product {product_id: $product_id})
                    CREATE (c)-[r:PURCHASED {
                        order_id: $order_id,
                        quantity: $quantity,
                        amount: $amount,
                        timestamp: datetime()
                    }]->(p)
                """, {
                    'customer_id': customer_id,
                    'product_id': product_id,
                    'order_id': order_data['order_id'],
                    'quantity': order_data['quantity'],
                    'amount': order_data['amount']
                })
                
                logger.info(
                    f"Recorded purchase: {customer_id} -> {product_id}"
                )
                
        except Exception as e:
            logger.error(f"Purchase recording error: {e}")
            raise
    
    def get_recommendations(
        self,
        customer_id: str,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Get personalized product recommendations."""
        try:
            with self.driver.session() as session:
                # Collaborative filtering approach
                result = session.run("""
                    MATCH (c:Customer {customer_id: $customer_id})
                    -[:PURCHASED]->(p:Product)
                    <-[:PURCHASED]-(other:Customer)
                    -[:PURCHASED]->(rec:Product)
                    WHERE NOT (c)-[:PURCHASED]->(rec)
                    WITH rec, count(*) as score,
                         collect(other.customer_id) as customers
                    ORDER BY score DESC
                    LIMIT $limit
                    RETURN rec.product_id as product_id,
                           rec.name as name,
                           rec.category as category,
                           score,
                           size(customers) as customer_count
                """, {
                    'customer_id': customer_id,
                    'limit': limit
                })
                
                recommendations = [
                    {
                        'product_id': record['product_id'],
                        'name': record['name'],
                        'category': record['category'],
                        'score': record['score'],
                        'customer_count': record['customer_count']
                    }
                    for record in result
                ]
                
                logger.info(
                    f"Generated recommendations for: {customer_id}"
                )
                return recommendations
                
        except Exception as e:
            logger.error(f"Recommendation error: {e}")
            raise
    
    def get_similar_products(
        self,
        product_id: str,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Find similar products based on purchase patterns."""
        try:
            with self.driver.session() as session:
                result = session.run("""
                    MATCH (p:Product {product_id: $product_id})
                    <-[:PURCHASED]-(c:Customer)
                    -[:PURCHASED]->(other:Product)
                    WHERE p <> other
                    WITH other,
                         count(*) as common_customers,
                         collect(c.customer_id) as customers
                    ORDER BY common_customers DESC
                    LIMIT $limit
                    RETURN other.product_id as product_id,
                           other.name as name,
                           other.category as category,
                           common_customers,
                           size(customers) as customer_count
                """, {
                    'product_id': product_id,
                    'limit': limit
                })
                
                similar_products = [
                    {
                        'product_id': record['product_id'],
                        'name': record['name'],
                        'category': record['category'],
                        'common_customers': record['common_customers'],
                        'customer_count': record['customer_count']
                    }
                    for record in result
                ]
                
                logger.info(
                    f"Found similar products for: {product_id}"
                )
                return similar_products
                
        except Exception as e:
            logger.error(f"Similar products error: {e}")
            raise
    
    def analyze_purchase_patterns(
        self,
        category: str = None,
        min_support: int = 10
    ) -> List[Dict[str, Any]]:
        """Analyze product purchase patterns."""
        try:
            with self.driver.session() as session:
                query = """
                    MATCH (p1:Product)<-[:PURCHASED]-
                          (c:Customer)-[:PURCHASED]->(p2:Product)
                    WHERE p1 <> p2
                """
                
                if category:
                    query += " AND p1.category = $category"
                
                query += """
                    WITH p1, p2, count(c) as frequency
                    WHERE frequency >= $min_support
                    RETURN p1.product_id as product1_id,
                           p1.name as product1_name,
                           p2.product_id as product2_id,
                           p2.name as product2_name,
                           frequency
                    ORDER BY frequency DESC
                    LIMIT 20
                """
                
                result = session.run(
                    query,
                    {
                        'category': category,
                        'min_support': min_support
                    }
                )
                
                patterns = [
                    {
                        'product1': {
                            'id': record['product1_id'],
                            'name': record['product1_name']
                        },
                        'product2': {
                            'id': record['product2_id'],
                            'name': record['product2_name']
                        },
                        'frequency': record['frequency']
                    }
                    for record in result
                ]
                
                logger.info("Analyzed purchase patterns")
                return patterns
                
        except Exception as e:
            logger.error(f"Pattern analysis error: {e}")
            raise
    
    def _disconnect(self) -> None:
        """Close Neo4j connection."""
        if hasattr(self, 'driver'):
            self.driver.close()
    
    def health_check(self) -> Dict[str, Any]:
        """Check Neo4j connection health."""
        try:
            with self.driver.session() as session:
                result = session.run("CALL dbms.components()")
                component = result.single()
                
                return {
                    'status': 'healthy',
                    'timestamp': datetime.utcnow(),
                    'version': component['versions'][0],
                    'edition': component['edition']
                }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.utcnow()
            } 