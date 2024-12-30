"""
Storage Optimization Tests
-----------------------

Tests storage optimization with:
1. Compression algorithms
2. Deduplication strategies
3. Performance metrics
"""

import unittest
from unittest.mock import Mock, patch, mock_open
import os
from pathlib import Path
import json
import zlib
import lz4.frame
import zstandard
import tempfile
import shutil
from storage_opt import StorageOptimizer

class TestStorageOptimizer(unittest.TestCase):
    """Test cases for storage optimization."""
    
    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.optimizer = StorageOptimizer(
            self.test_dir,
            block_size=1024,
            threads=2
        )
        
        # Create test data
        self.test_data = b"Test data" * 1000
        self.test_file = Path(self.test_dir) / "test.txt"
        with open(self.test_file, "wb") as f:
            f.write(self.test_data)
    
    def test_compress_file_zlib(self):
        """Test file compression with zlib."""
        result = self.optimizer.compress_file(
            self.test_file,
            algorithm='zlib'
        )
        
        self.assertTrue(result['compressed_path'].endswith('.zlib'))
        self.assertGreater(result['ratio'], 1.0)
        self.assertLess(result['compressed_size'],
                       result['original_size'])
    
    def test_compress_file_lz4(self):
        """Test file compression with LZ4."""
        result = self.optimizer.compress_file(
            self.test_file,
            algorithm='lz4'
        )
        
        self.assertTrue(result['compressed_path'].endswith('.lz4'))
        self.assertGreater(result['ratio'], 1.0)
    
    def test_compress_file_zstd(self):
        """Test file compression with Zstandard."""
        result = self.optimizer.compress_file(
            self.test_file,
            algorithm='zstd'
        )
        
        self.assertTrue(result['compressed_path'].endswith('.zstd'))
        self.assertGreater(result['ratio'], 1.0)
    
    def test_auto_algorithm_selection(self):
        """Test automatic algorithm selection."""
        result = self.optimizer.compress_file(
            self.test_file,
            algorithm='auto'
        )
        
        self.assertIn(
            result['algorithm'],
            ['zlib', 'lz4', 'zstd']
        )
    
    def test_decompress_file(self):
        """Test file decompression."""
        # First compress
        compressed = self.optimizer.compress_file(
            self.test_file,
            algorithm='zlib'
        )
        
        # Then decompress
        result = self.optimizer.decompress_file(
            compressed['compressed_path']
        )
        
        # Verify content
        with open(result['output_path'], 'rb') as f:
            decompressed_data = f.read()
        
        self.assertEqual(decompressed_data, self.test_data)
    
    def test_deduplicate_file(self):
        """Test file deduplication."""
        # Create file with repeating blocks
        dedup_data = b"Block1" * 1000 + b"Block2" * 1000
        dedup_file = Path(self.test_dir) / "dedup.txt"
        with open(dedup_file, "wb") as f:
            f.write(dedup_data)
        
        result = self.optimizer.deduplicate_file(dedup_file)
        
        self.assertGreater(result['space_saved'], 0)
        self.assertLess(
            result['unique_blocks'],
            result['total_blocks']
        )
    
    def test_analyze_storage(self):
        """Test storage analysis."""
        # Create test directory structure
        test_files = {
            "file1.txt": b"Content1" * 100,
            "file2.txt": b"Content2" * 100,
            "data.json": b'{"key": "value"}',
            "duplicate.txt": b"Content1" * 100
        }
        
        for name, content in test_files.items():
            path = Path(self.test_dir) / name
            with open(path, "wb") as f:
                f.write(content)
        
        result = self.optimizer.analyze_storage(self.test_dir)
        
        self.assertEqual(result['file_count'], 5)  # Including test.txt
        self.assertGreater(result['total_size'], 0)
        self.assertIn('.txt', result['extension_stats'])
        self.assertGreater(len(result['duplicates']), 0)
    
    def test_error_handling(self):
        """Test error handling."""
        # Test non-existent file
        with self.assertRaises(FileNotFoundError):
            self.optimizer.compress_file(
                "nonexistent.txt"
            )
        
        # Test invalid algorithm
        with self.assertRaises(ValueError):
            self.optimizer.compress_file(
                self.test_file,
                algorithm='invalid'
            )
        
        # Test invalid decompression
        invalid_file = Path(self.test_dir) / "invalid.zlib"
        with open(invalid_file, "wb") as f:
            f.write(b"Invalid compressed data")
        
        with self.assertRaises(Exception):
            self.optimizer.decompress_file(invalid_file)
    
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)

if __name__ == '__main__':
    unittest.main() 