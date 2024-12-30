"""
Test specialized storage operations.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import shutil
import os
import numpy as np
import pandas as pd
from datetime import datetime
import networkx as nx
from pyspark.sql import SparkSession
from exercises.specialized_storage import (
    ImageStorage,
    TimeSeriesStorage,
    GraphStorage
)

class TestSpecializedStorage(unittest.TestCase):
    """Test specialized storage functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        cls.spark = SparkSession.builder \
            .appName("TestSpecializedStorage") \
            .master("local[*]") \
            .getOrCreate()
        
        cls.temp_dir = tempfile.mkdtemp()
        
        # Create test time series data
        cls.ts_data = cls.spark.createDataFrame([
            ("2023-01-01 00:00:00", 1.1),
            ("2023-01-01 01:00:00", 2.2),
            ("2023-01-01 02:00:00", 3.3)
        ], ["timestamp", "value"])
        
        # Create test graph data
        cls.nodes = cls.spark.createDataFrame([
            (1, "Node1"),
            (2, "Node2"),
            (3, "Node3")
        ], ["id", "name"])
        
        cls.edges = cls.spark.createDataFrame([
            (1, 2, "edge1"),
            (2, 3, "edge2")
        ], ["src", "dst", "label"])
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        shutil.rmtree(cls.temp_dir)
        cls.spark.stop()
    
    def test_image_storage(self):
        """Test image storage."""
        storage = ImageStorage(os.path.join(self.temp_dir, "images"))
        
        # Test image storage
        test_image = b"test_image_data"
        test_metadata = {"label": "test"}
        test_embedding = np.random.rand(128)
        
        storage.store_image(
            test_image,
            test_metadata,
            test_embedding
        )
        
        # Test similarity search
        query = np.random.rand(128)
        results = storage.search_similar(query, k=1)
        
        self.assertEqual(len(results), 1)
        self.assertIn('image', results[0])
        self.assertIn('metadata', results[0])
    
    def test_time_series_storage(self):
        """Test time series storage."""
        storage = TimeSeriesStorage(self.spark)
        
        # Test partitioning
        partitioned = storage.optimize_partitioning(
            self.ts_data,
            "timestamp",
            "month"
        )
        
        self.assertTrue("year" in partitioned.columns)
        self.assertTrue("month" in partitioned.columns)
        
        # Test downsampling
        downsampled = storage.downsample_series(
            self.ts_data,
            "timestamp",
            "value",
            "1 hour"
        )
        
        self.assertTrue("value_avg" in downsampled.columns)
        self.assertTrue("value_min" in downsampled.columns)
        self.assertTrue("value_max" in downsampled.columns)
    
    def test_graph_storage(self):
        """Test graph storage."""
        storage = GraphStorage(self.spark)
        
        # Test GraphFrames storage
        graph_frames = storage.store_graph(
            self.nodes,
            self.edges,
            "graphframes"
        )
        self.assertIsNotNone(graph_frames)
        
        # Test NetworkX storage
        nx_graph = storage.store_graph(
            self.nodes,
            self.edges,
            "networkx"
        )
        self.assertIsInstance(nx_graph, nx.Graph)
        
        # Test traversal optimization
        optimized = storage.optimize_traversal(nx_graph, "bfs")
        self.assertIsNotNone(optimized)
        
        # Test structure analysis
        analysis = storage.analyze_structure(nx_graph)
        self.assertIn('num_nodes', analysis)
        self.assertIn('num_edges', analysis)
        self.assertIn('density', analysis)
    
    def test_invalid_graph_format(self):
        """Test invalid graph format handling."""
        storage = GraphStorage(self.spark)
        
        with self.assertRaises(ValueError):
            storage.store_graph(
                self.nodes,
                self.edges,
                "invalid_format"
            )
    
    def test_time_series_partitioning(self):
        """Test different partitioning frequencies."""
        storage = TimeSeriesStorage(self.spark)
        
        # Test daily partitioning
        daily = storage.optimize_partitioning(
            self.ts_data,
            "timestamp",
            "day"
        )
        self.assertTrue("day" in daily.columns)
        
        # Test monthly partitioning
        monthly = storage.optimize_partitioning(
            self.ts_data,
            "timestamp",
            "month"
        )
        self.assertTrue("month" in monthly.columns)
        self.assertFalse("day" in monthly.columns)
    
    def test_graph_analysis(self):
        """Test detailed graph analysis."""
        storage = GraphStorage(self.spark)
        
        # Create more complex graph
        nodes = self.spark.createDataFrame([
            (1, "A"), (2, "B"), (3, "C"),
            (4, "D"), (5, "E")
        ], ["id", "name"])
        
        edges = self.spark.createDataFrame([
            (1, 2), (2, 3), (3, 4),
            (4, 5), (5, 1)
        ], ["src", "dst"])
        
        # Create NetworkX graph
        graph = storage.store_graph(nodes, edges, "networkx")
        
        # Analyze structure
        analysis = storage.analyze_structure(graph)
        
        # Verify metrics
        self.assertEqual(analysis['num_nodes'], 5)
        self.assertEqual(analysis['num_edges'], 5)
        self.assertGreater(analysis['density'], 0)
        self.assertEqual(analysis['connected_components'], 1)

if __name__ == '__main__':
    unittest.main() 