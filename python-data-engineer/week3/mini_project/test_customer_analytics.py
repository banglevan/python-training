"""Unit tests for Customer Analytics."""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
import shutil
from customer_analytics import CustomerAnalytics

class TestCustomerAnalytics(unittest.TestCase):
    """Test cases for Customer Analytics."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.analytics = CustomerAnalytics("TestCustomerAnalytics")
        
        # Create test data directory
        os.makedirs("test_data", exist_ok=True)
        
        # Create sample customer data
        customer_data = [
            (1, "John", "NY", 30),
            (2, "Jane", "CA", 25),
            (3, "Bob", "TX", 35)
        ]
        
        customer_schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("state", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        
        cls.customers_df = cls.analytics.spark.createDataFrame(
            customer_data,
            customer_schema
        )
        
        # Create sample transaction data
        transaction_data = [
            (1, 1, "P1", 100, "2024-01-01"),
            (2, 1, "P2", 200, "2024-01-15"),
            (3, 2, "P1", 150, "2024-01-10"),
            (4, 3, "P3", 300, "2024-01-05")
        ]
        
        transaction_schema = StructType([
            StructField("transaction_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("product_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("transaction_date", StringType(), True)
        ])
        
        cls.transactions_df = cls.analytics.spark.createDataFrame(
            transaction_data,
            transaction_schema
        )
        
        # Save test data
        cls.customers_df.write.parquet(
            "test_data/customers.parquet"
        )
        cls.transactions_df.write.parquet(
            "test_data/transactions.parquet"
        )
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        if cls.analytics:
            cls.analytics.cleanup()
        
        # Remove test data directory
        shutil.rmtree("test_data")
    
    def test_load_customer_data(self):
        """Test customer data loading."""
        df = self.analytics.load_customer_data(
            "test_data/transactions.parquet",
            "test_data/customers.parquet"
        )
        
        self.assertIsNotNone(df)
        self.assertTrue("customer_id" in df.columns)
        self.assertTrue("transaction_id" in df.columns)
        self.assertTrue("name" in df.columns)
    
    def test_calculate_customer_metrics(self):
        """Test customer metrics calculation."""
        df = self.analytics.load_customer_data(
            "test_data/transactions.parquet",
            "test_data/customers.parquet"
        )
        
        metrics_df = self.analytics.calculate_customer_metrics(df)
        
        self.assertIsNotNone(metrics_df)
        self.assertTrue(
            "days_since_last_purchase" in metrics_df.columns
        )
        self.assertTrue(
            "purchase_frequency" in metrics_df.columns
        )
        self.assertTrue(
            "total_amount" in metrics_df.columns
        )
    
    def test_segment_customers(self):
        """Test customer segmentation."""
        df = self.analytics.load_customer_data(
            "test_data/transactions.parquet",
            "test_data/customers.parquet"
        )
        metrics_df = self.analytics.calculate_customer_metrics(df)
        
        segmented_df, model = self.analytics.segment_customers(
            metrics_df,
            num_clusters=3
        )
        
        self.assertIsNotNone(segmented_df)
        self.assertTrue("segment" in segmented_df.columns)
        self.assertIsNotNone(model)
    
    def test_analyze_behavior(self):
        """Test behavior analysis."""
        df = self.analytics.load_customer_data(
            "test_data/transactions.parquet",
            "test_data/customers.parquet"
        )
        metrics_df = self.analytics.calculate_customer_metrics(df)
        segmented_df, _ = self.analytics.segment_customers(
            metrics_df
        )
        
        patterns = self.analytics.analyze_behavior(
            df,
            segmented_df
        )
        
        self.assertIsNotNone(patterns)
        self.assertIsInstance(patterns, dict)
        self.assertTrue(len(patterns) > 0)
    
    def test_generate_insights(self):
        """Test insights generation."""
        df = self.analytics.load_customer_data(
            "test_data/transactions.parquet",
            "test_data/customers.parquet"
        )
        metrics_df = self.analytics.calculate_customer_metrics(df)
        segmented_df, _ = self.analytics.segment_customers(
            metrics_df
        )
        patterns = self.analytics.analyze_behavior(
            df,
            segmented_df
        )
        
        insights = self.analytics.generate_insights(
            segmented_df,
            patterns
        )
        
        self.assertIsNotNone(insights)
        self.assertIn("segment_profiles", insights)
        self.assertIn("recommendations", insights)
    
    def test_save_results(self):
        """Test results saving."""
        df = self.analytics.load_customer_data(
            "test_data/transactions.parquet",
            "test_data/customers.parquet"
        )
        metrics_df = self.analytics.calculate_customer_metrics(df)
        segmented_df, _ = self.analytics.segment_customers(
            metrics_df
        )
        patterns = self.analytics.analyze_behavior(
            df,
            segmented_df
        )
        insights = self.analytics.generate_insights(
            segmented_df,
            patterns
        )
        
        output_path = "test_data/output"
        self.analytics.save_results(
            segmented_df,
            patterns,
            insights,
            output_path
        )
        
        self.assertTrue(
            os.path.exists(f"{output_path}/segmented_customers")
        )
        self.assertTrue(
            os.path.exists(f"{output_path}/patterns")
        )
        self.assertTrue(
            os.path.exists(f"{output_path}/insights")
        )

if __name__ == '__main__':
    unittest.main() 