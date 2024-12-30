"""
Test Apache Hudi features.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import os
from datetime import datetime
from pyspark.sql.types import *

from exercises.hudi_features import HudiFeatures

class TestHudiFeatures(unittest.TestCase):
    """Test Hudi advanced features."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        # Create Hudi features instance
        cls.hudi = HudiFeatures()
        
        # Create temporary directory
        cls.temp_dir = tempfile.mkdtemp()
        
        # Define schema
        cls.schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("ts", TimestampType(), True),
            StructField("date", DateType(), True)
        ])
        
        # Create test table and data
        cls.table_path = os.path.join(cls.temp_dir, "test_table")
        test_data = cls.hudi.spark.createDataFrame([
            (1, "A", 10.0, datetime.now(), datetime.now().date()),
            (2, "B", 20.0, datetime.now(), datetime.now().date())
        ], cls.schema)
        
        # Write test data
        test_data.write.format("hudi") \
            .option("hoodie.table.name", "test_table") \
            .option("hoodie.datasource.write.recordkey.field", "id") \
            .option("hoodie.datasource.write.precombine.field", "ts") \
            .mode("overwrite") \
            .save(cls.table_path)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Stop Spark session
        cls.hudi.spark.stop()
        
        # Remove temporary directory
        shutil.rmtree(cls.temp_dir)
    
    def test_schedule_clustering(self):
        """Test clustering scheduling."""
        # Schedule clustering
        self.hudi.schedule_clustering(
            self.table_path,
            cluster_columns=["date", "id"]
        )
        
        # Verify through commit metadata
        commits = self.hudi.get_commit_metadata(self.table_path)
        self.assertTrue(
            any("CLUSTERING" in str(commit) for commit in commits)
        )
    
    def test_schedule_compaction(self):
        """Test compaction scheduling."""
        # Schedule compaction
        self.hudi.schedule_compaction(self.table_path)
        
        # Verify through commit metadata
        commits = self.hudi.get_commit_metadata(self.table_path)
        self.assertTrue(
            any("COMPACTION" in str(commit) for commit in commits)
        )
    
    def test_run_clean(self):
        """Test cleaning operation."""
        # Run cleaning
        self.hudi.run_clean(
            self.table_path,
            retain_commits=5,
            hours=24
        )
        
        # Verify through commit metadata
        commits = self.hudi.get_commit_metadata(self.table_path)
        self.assertTrue(
            any("CLEAN" in str(commit) for commit in commits)
        )
    
    def test_get_commit_metadata(self):
        """Test commit metadata retrieval."""
        # Get commit metadata
        commits = self.hudi.get_commit_metadata(
            self.table_path,
            limit=5
        )
        
        # Verify metadata structure
        self.assertTrue(len(commits) > 0)
        self.assertTrue(all(
            'commitTime' in commit 
            for commit in commits
        ))
    
    def test_set_table_config(self):
        """Test table configuration."""
        # Set config
        config = {
            'hoodie.cleaner.commits.retained': '5',
            'hoodie.compact.inline.max.delta.commits': '5'
        }
        
        self.hudi.set_table_config(
            self.table_path,
            config
        )
        
        # Verify config through table properties
        table_config = self.hudi.spark.sql(f"""
            SHOW TBLPROPERTIES {self.table_path}
        """).collect()
        
        for key, value in config.items():
            self.assertTrue(
                any(
                    row.key == key and row.value == value 
                    for row in table_config
                )
            )
    
    def test_error_handling(self):
        """Test error handling scenarios."""
        # Test invalid clustering columns
        with self.assertRaises(Exception):
            self.hudi.schedule_clustering(
                self.table_path,
                cluster_columns=["invalid_column"]
            )
        
        # Test invalid compaction instant time
        with self.assertRaises(Exception):
            self.hudi.schedule_compaction(
                self.table_path,
                instant_time="invalid_time"
            )
        
        # Test invalid clean parameters
        with self.assertRaises(Exception):
            self.hudi.run_clean(
                self.table_path,
                retain_commits=-1
            )
        
        # Test invalid table config
        with self.assertRaises(Exception):
            self.hudi.set_table_config(
                self.table_path,
                {'invalid.config': 'value'}
            )

if __name__ == '__main__':
    unittest.main() 