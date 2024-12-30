"""
Test data processors.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from processors.image import ImageProcessor
from processors.metadata import MetadataProcessor
from processors.enrichment import DataEnrichment

class TestProcessors(unittest.TestCase):
    """Test processor functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        cls.spark = SparkSession.builder \
            .appName("TestProcessors") \
            .master("local[*]") \
            .getOrCreate()
        
        cls.temp_dir = tempfile.mkdtemp()
        
        # Create test image
        cls.test_image = b"test_image_content"
        
        # Create test schema
        cls.schema = StructType([
            StructField("id", StringType(), False),
            StructField("content", BinaryType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])
        
        # Create test data
        cls.test_data = cls.spark.createDataFrame([
            ("1", cls.test_image, {"format": "JPEG", "size": "1024"})
        ], cls.schema)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.spark.stop()
        shutil.rmtree(cls.temp_dir)
    
    def test_image_processing(self):
        """Test image processing."""
        processor = ImageProcessor(self.spark)
        
        # Test resizing
        with patch('PIL.Image.open') as mock_open:
            mock_img = Mock()
            mock_img.format = 'JPEG'
            mock_open.return_value = mock_img
            
            result = processor.resize_images(
                self.test_data,
                (100, 100)
            )
            self.assertEqual(result.count(), 1)
    
    def test_metadata_processing(self):
        """Test metadata processing."""
        processor = MetadataProcessor(self.spark)
        
        # Test metadata extraction
        with patch('PIL.Image.open') as mock_open:
            mock_img = Mock()
            mock_img.format = 'JPEG'
            mock_img.mode = 'RGB'
            mock_img.width = 100
            mock_img.height = 100
            mock_open.return_value = mock_img
            
            result = processor.extract_image_info(
                self.test_data
            )
            self.assertEqual(result.count(), 1)
    
    def test_data_enrichment(self):
        """Test data enrichment."""
        enrichment = DataEnrichment(self.spark)
        
        # Test technical metadata
        result = enrichment.add_technical_metadata(
            self.test_data
        )
        self.assertTrue("content_hash" in result.columns)
        
        # Test business metadata
        rules = {
            "size_rules": {
                "small": 1024,
                "medium": 1024 * 1024
            }
        }
        result = enrichment.add_business_metadata(
            result,
            rules
        )
        self.assertTrue("size_category" in result.columns)

if __name__ == '__main__':
    unittest.main() 