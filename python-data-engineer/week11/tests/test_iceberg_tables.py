"""
Test Apache Iceberg table operations.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import os
from datetime import datetime
from pyspark.sql.types import *

from exercises.iceberg_tables import IcebergTables

class TestIcebergTables(unittest.TestCase):
    """Test Iceberg table operations."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        # Create Iceberg instance
        cls.iceberg = IcebergTables()
        
        # Create temporary warehouse directory
        cls.temp_dir = tempfile.mkdtemp()
        cls.iceberg.spark.conf.set(
            "spark.sql.catalog.local.warehouse",
            cls.temp_dir
        )
        
        # Define test schema
        cls.schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("date", DateType(), True)
        ])
        
        # Create test data
        cls.test_data = cls.iceberg.spark.createDataFrame([
            (1, "A", 10.0, datetime.now().date()),
            (2, "B", 20.0, datetime.now().date())
        ], cls.schema)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Stop Spark session
        cls.iceberg.spark.stop()
        
        # Remove temporary directory
        shutil.rmtree(cls.temp_dir)
    
    def test_create_table(self):
        """Test Iceberg table creation."""
        # Create table
        self.iceberg.create_table(
            "local.db.create_test",
            self.schema,
            partition_by=["date"],
            properties={
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "snappy"
            }
        )
        
        # Verify table exists
        tables = self.iceberg.spark.sql(
            "SHOW TABLES IN local.db"
        ).collect()
        self.assertTrue(
            any(row.tableName == "create_test" for row in tables)
        )
    
    def test_write_data(self):
        """Test writing data to Iceberg table."""
        table_name = "local.db.write_test"
        
        # Create table
        self.iceberg.create_table(table_name, self.schema)
        
        # Write data
        self.iceberg.write_data(table_name, self.test_data)
        
        # Verify data
        result = self.iceberg.spark.table(table_name)
        self.assertEqual(result.count(), 2)
        self.assertEqual(
            result.select("name").collect(),
            [("A",), ("B",)]
        )
    
    def test_optimize_table(self):
        """Test table optimization."""
        table_name = "local.db.optimize_test"
        
        # Create and write data
        self.iceberg.create_table(table_name, self.schema)
        self.iceberg.write_data(table_name, self.test_data)
        
        # Optimize
        self.iceberg.optimize_table(
            table_name,
            sort_by=["id"]
        )
        
        # Verify optimization through table properties
        properties = self.iceberg.spark.sql(f"""
            DESCRIBE TABLE EXTENDED {table_name}
        """).collect()
        
        self.assertTrue(
            any("sort.order" in str(row) for row in properties)
        )
    
    def test_expire_snapshots(self):
        """Test snapshot expiration."""
        table_name = "local.db.expire_test"
        
        # Create and write data multiple times
        self.iceberg.create_table(table_name, self.schema)
        for _ in range(3):
            self.iceberg.write_data(table_name, self.test_data)
        
        # Expire snapshots
        self.iceberg.expire_snapshots(
            table_name,
            retention_hours=0
        )
        
        # Verify through snapshots table
        snapshots = self.iceberg.spark.sql(f"""
            SELECT * FROM {table_name}.snapshots
        """).collect()
        
        # Should only have the latest snapshot
        self.assertEqual(len(snapshots), 1)
    
    def test_analyze_table(self):
        """Test table analysis."""
        table_name = "local.db.analyze_test"
        
        # Create and write data
        self.iceberg.create_table(table_name, self.schema)
        self.iceberg.write_data(table_name, self.test_data)
        
        # Get statistics
        stats = self.iceberg.analyze_table(table_name)
        
        # Verify statistics
        self.assertTrue("id" in stats)
        self.assertTrue("name" in stats)
        self.assertTrue("value" in stats)
        self.assertTrue("date" in stats)
    
    def test_error_handling(self):
        """Test error handling."""
        # Test invalid table name
        with self.assertRaises(Exception):
            self.iceberg.create_table(
                "invalid.table.name",
                self.schema
            )
        
        # Test invalid schema
        with self.assertRaises(Exception):
            self.iceberg.create_table(
                "local.db.error_test",
                "invalid_schema"
            )
        
        # Test invalid partition column
        with self.assertRaises(Exception):
            self.iceberg.create_table(
                "local.db.error_test",
                self.schema,
                partition_by=["non_existent_column"]
            )

if __name__ == '__main__':
    unittest.main() 