"""
Unit Tests for Neo4j Graph Operations
----------------------------------

Test Coverage:
1. Node Operations
   - Creation/Deletion
   - Property management
   - Label handling

2. Relationship Management
   - Creation/Validation
   - Property updates
   - Direction verification

3. Graph Algorithms
   - Path finding
   - Centrality metrics
   - Community detection
"""

import unittest
from neo4j_graph import Neo4jGraph
from datetime import datetime
import time

class TestNeo4jGraph(unittest.TestCase):
    """Test cases for Neo4j graph operations."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.graph = Neo4jGraph()
        
        # Clear existing data
        with cls.graph.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        with cls.graph.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        cls.graph.close()
    
    def test_node_operations(self):
        """Test node creation and management."""
        # Create node with properties
        person = self.graph.create_node(
            'Person',
            {
                'name': 'John Doe',
                'age': 30,
                'email': 'john@example.com'
            },
            constraints=['email']
        )
        
        self.assertIsNotNone(person)
        
        # Verify node properties
        with self.graph.driver.session() as session:
            result = session.run("""
                MATCH (p:Person {email: $email})
                RETURN p
            """, email='john@example.com')
            
            node = result.single()['p']
            self.assertEqual(node['name'], 'John Doe')
            self.assertEqual(node['age'], 30)
    
    def test_relationship_creation(self):
        """Test relationship management."""
        # Create test nodes
        self.graph.create_node(
            'Person',
            {'name': 'Alice', 'id': '1'}
        )
        self.graph.create_node(
            'Person',
            {'name': 'Bob', 'id': '2'}
        )
        
        # Create relationship
        rel = self.graph.create_relationship(
            'Person',
            'KNOWS',
            'Person',
            {'since': '2023'},
            where={'name': 'Alice'}
        )
        
        self.assertIsNotNone(rel)
        
        # Verify relationship
        with self.graph.driver.session() as session:
            result = session.run("""
                MATCH (a:Person {name: 'Alice'})
                      -[r:KNOWS]->
                      (b:Person {name: 'Bob'})
                RETURN r
            """)
            
            relationship = result.single()['r']
            self.assertEqual(
                relationship['since'],
                '2023'
            )
    
    def test_path_finding(self):
        """Test path finding algorithms."""
        # Create test graph
        nodes = [
            ('A', 'Person', {'name': f'Person_{i}'})
            for i in range(5)
        ]
        
        for node_id, label, props in nodes:
            self.graph.create_node(label, props)
        
        # Create relationships
        relationships = [
            ('Person_0', 'KNOWS', 'Person_1'),
            ('Person_1', 'KNOWS', 'Person_2'),
            ('Person_2', 'KNOWS', 'Person_3'),
            ('Person_1', 'KNOWS', 'Person_4')
        ]
        
        for start, rel_type, end in relationships:
            self.graph.create_relationship(
                'Person',
                rel_type,
                'Person',
                where={'name': start}
            )
        
        # Find paths
        paths = self.graph.query_paths(
            'Person',
            'Person',
            'KNOWS',
            max_depth=3,
            where={'name': 'Person_0'}
        )
        
        self.assertTrue(len(paths) > 0)
        
        # Verify path lengths
        path_lengths = [p['length'] for p in paths]
        self.assertTrue(all(l <= 3 for l in path_lengths))
    
    def test_graph_algorithms(self):
        """Test graph algorithms execution."""
        # Create test social network
        people = [
            ('Alice', 3),
            ('Bob', 2),
            ('Charlie', 4),
            ('David', 1)
        ]
        
        for name, connections in people:
            self.graph.create_node(
                'Person',
                {'name': name}
            )
        
        # Create relationships
        relationships = [
            ('Alice', 'Bob'),
            ('Alice', 'Charlie'),
            ('Bob', 'Charlie'),
            ('Charlie', 'David')
        ]
        
        for start, end in relationships:
            self.graph.create_relationship(
                'Person',
                'KNOWS',
                'Person',
                where={'name': start}
            )
        
        # Run PageRank
        results = self.graph.run_algorithm(
            'pagerank',
            {'nodeLabel': 'Person'}
        )
        
        self.assertTrue(len(results) > 0)
        
        # Verify centrality
        scores = {r['name']: r['score'] for r in results}
        self.assertGreater(
            scores['Charlie'],
            scores['David'],
            "Charlie should have higher PageRank"
        )
    
    def test_performance_monitoring(self):
        """Test performance monitoring."""
        # Generate test data
        for i in range(100):
            self.graph.create_node(
                'TestNode',
                {
                    'id': str(i),
                    'timestamp': datetime.now().isoformat()
                }
            )
        
        # Get statistics
        stats = self.graph.monitor_stats()
        
        # Verify metrics
        self.assertIn('nodes', stats)
        self.assertIn('relationships', stats)
        self.assertIn('labels', stats)
        self.assertGreaterEqual(stats['nodes'], 100)
        self.assertIn('TestNode', stats['labels'])

if __name__ == '__main__':
    unittest.main() 