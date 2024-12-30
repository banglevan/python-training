"""
Test Apache Iceberg features.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import os
from datetime import datetime
from pyspark.sql.types import *

from exercises.iceberg_features import IcebergFeatures

class TestIcebergFeatures(unittest.TestCase):
    """Test Iceberg advanced features."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        # Create Iceberg features instance
        cls.iceberg = IcebergFeatures()
        
        # Create temporary warehouse directory
        cls.temp_dir = tempfile.mkdtemp()
        cls.iceberg.spark.conf.set(
            "spark.sql.catalog.local.warehouse",
            cls.temp_dir
        )
        
        # Define schema
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
        
        # Create test table
        cls.table_name = "local.db.test_table"
        cls.test_data.writeTo(cls.table_name) \
            .using("iceberg") \
            .createOrReplace()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Stop Spark session
        cls.iceberg.spark.stop()
        
        # Remove temporary directory
        shutil.rmtree(cls.temp_dir)
    
    def test_manage_snapshots(self):
        """Test snapshot management."""
        # Write more data to create new snapshot
        new_data = self.iceberg.spark.createDataFrame([
            (3, "C", 30.0, datetime.now().date())
        ], self.schema)
        
        new_data.writeTo(self.table_name) \
            .using("iceberg") \
            .append()
        
        # Get snapshot ID
        snapshots = self.iceberg.get_snapshot_info(self.table_name)
        snapshot_id = snapshots[0]['snapshot_id']
        
        # Test rollback
        self.iceberg.manage_snapshots(
            self.table_name,
            "rollback",
            snapshot_id
        )
        
        # Verify rollback
        current_data = self.iceberg.spark.table(self.table_name)
        self.assertEqual(current_data.count(), 3)
    
    def test_update_partition_spec(self):
        """Test partition specification updates."""
        # Update partition spec
        self.iceberg.update_partition_spec(
            self.table_name,
            {
                "date": "day",
                "id": "bucket[16]"
            }
        )
        
        # Verify partition spec
        table_desc = self.iceberg.spark.sql(f"""
            DESCRIBE TABLE EXTENDED {self.table_name}
        """).collect()
        
        partition_info = [row for row in table_desc 
                         if "Partition Provider" in str(row)]
        self.assertTrue(len(partition_info) > 0)
    
    def test_maintain_table(self):
        """Test table maintenance operations."""
        # Write more data
        for i in range(3):
            new_data = self.iceberg.spark.createDataFrame([
                (i+10, f"Test{i}", i*10.0, datetime.now().date())
            ], self.schema)
            new_data.writeTo(self.table_name) \
                .using("iceberg") \
                .append()
        
        # Perform maintenance
        self.iceberg.maintain_table(
            self.table_name,
            ["remove_orphans", "compact", "expire_snapshots"]
        )
        
        # Verify maintenance through snapshots
        snapshots = self.iceberg.get_snapshot_info(self.table_name)
        self.assertTrue(len(snapshots) >= 1)  # Should have at least one snapshot
    
    def test_get_snapshot_info(self):
        """Test snapshot information retrieval."""
        # Get snapshots
        snapshots = self.iceberg.get_snapshot_info(
            self.table_name,
            limit=5
        )
        
        # Verify snapshot structure
        self.assertTrue(len(snapshots) > 0)
        self.assertTrue(all(
            'snapshot_id' in snapshot 
            for snapshot in snapshots
        ))
        self.assertTrue(all(
            'committed_at' in snapshot 
            for snapshot in snapshots
        ))
    
    def test_set_table_properties(self):
        """Test setting table properties."""
        # Set properties
        properties = {
            "write.metadata.compression-codec": "gzip",
            "write.metadata.metrics.default": "full",
            "write.format.default": "parquet"
        }
        
        self.iceberg.set_table_properties(
            self.table_name,
            properties
        )
        
        # Verify properties
        table_props = self.iceberg.spark.sql(f"""
            SHOW TBLPROPERTIES {self.table_name}
        """).collect()
        
        for key, value in properties.items():
            self.assertTrue(
                any(
                    row.key == key and row.value == value 
                    for row in table_props
                )
            )
    
    def test_error_handling(self):
        """Test error handling scenarios."""
        # Test invalid snapshot ID
        with self.assertRaises(Exception):
            self.iceberg.manage_snapshots(
                self.table_name,
                "rollback",
                "invalid_id"
            )
        
        # Test invalid partition spec
        with self.assertRaises(Exception):
            self.iceberg.update_partition_spec(
                self.table_name,
                {"invalid_column": "day"}
            )
        
        # Test invalid maintenance action
        with self.assertLogs(level='WARNING'):
            self.iceberg.maintain_table(
                self.table_name,
                ["invalid_action"]
            )
        
        # Test invalid table properties
        with self.assertRaises(Exception):
            self.iceberg.set_table_properties(
                self.table_name,
                {"invalid.property": None}
            )

if __name__ == '__main__':
    unittest.main() 