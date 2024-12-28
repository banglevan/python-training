"""Unit tests for Data Processing."""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import shutil
from data_processing import DataProcessor

class TestDataProcessor(unittest.TestCase):
    """Test cases for Data Processor."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.processor = DataProcessor("TestDataProcessor")
        
        # Create test data directory
        os.makedirs("test_data", exist_ok=True)
        
        # Create sample CSV file
        with open("test_data/sample.csv", "w") as f:
            f.write("id,value\n")
            f.write("1,100\n")
            f.write("2,200\n")
            f.write("3,300\n")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        if cls.processor:
            cls.processor.cleanup()
        
        # Remove test data directory
        shutil.rmtree("test_data")
    
    def test_load_data(self):
        """Test data loading."""
        df = self.processor.load_data(
            "test_data/sample.csv",
            format="csv",
            options={"header": "true", "inferSchema": "true"}
        )
        
        self.assertIsNotNone(df)
        self.assertEqual(df.count(), 3)
        self.assertTrue("id" in df.columns)
        self.assertTrue("value" in df.columns)
    
    def test_apply_transformations(self):
        """Test transformations."""
        # Load test data
        df = self.processor.load_data(
            "test_data/sample.csv",
            format="csv",
            options={"header": "true", "inferSchema": "true"}
        )
        
        transformations = [
            {
                "type": "select",
                "params": {"columns": ["id", "value"]}
            },
            {
                "type": "filter",
                "params": {"condition": "value > 100"}
            },
            {
                "type": "withColumn",
                "params": {
                    "name": "doubled_value",
                    "expression": F.col("value") * 2
                }
            }
        ]
        
        result = self.processor.apply_transformations(
            df,
            transformations
        )
        
        self.assertIsNotNone(result)
        self.assertTrue("doubled_value" in result.columns)
        self.assertTrue(result.count() < 3)
    
    def test_optimize_execution(self):
        """Test execution optimization."""
        df = self.processor.load_data(
            "test_data/sample.csv",
            format="csv",
            options={"header": "true", "inferSchema": "true"}
        )
        
        result = self.processor.optimize_execution(df)
        
        self.assertIsNotNone(result)
        self.assertEqual(
            result.rdd.getNumPartitions(),
            max(10, df.rdd.getNumPartitions())
        )
    
    def test_execute_actions(self):
        """Test action execution."""
        df = self.processor.load_data(
            "test_data/sample.csv",
            format="csv",
            options={"header": "true", "inferSchema": "true"}
        )
        
        actions = [
            {"type": "count"},
            {
                "type": "write",
                "params": {
                    "path": "test_data/output",
                    "format": "parquet",
                    "mode": "overwrite"
                }
            }
        ]
        
        results = self.processor.execute_actions(df, actions)
        
        self.assertIsNotNone(results)
        self.assertIn("count", results)
        self.assertEqual(results["count"], 3)
        self.assertTrue(
            os.path.exists("test_data/output")
        )
    
    def test_error_handling(self):
        """Test error handling."""
        # Test with invalid file path
        with self.assertRaises(Exception):
            self.processor.load_data(
                "nonexistent/file.csv"
            )
        
        # Test with invalid transformation
        df = self.processor.load_data(
            "test_data/sample.csv",
            format="csv",
            options={"header": "true", "inferSchema": "true"}
        )
        
        with self.assertRaises(Exception):
            self.processor.apply_transformations(
                df,
                [{"type": "invalid_operation"}]
            )
        
        # Test with invalid action
        with self.assertRaises(Exception):
            self.processor.execute_actions(
                df,
                [{"type": "invalid_action"}]
            )

if __name__ == '__main__':
    unittest.main() 