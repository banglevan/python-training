"""
File System Operations Tests
--------------------------

Tests file system operations with:
1. File operations
2. Directory operations
3. Permission management
4. Error handling
"""

import unittest
import os
import shutil
import tempfile
from pathlib import Path
from datetime import datetime
from fs_operations import FileSystemOps

class TestFileSystemOps(unittest.TestCase):
    """Test cases for FileSystemOps."""
    
    def setUp(self):
        """Set up test environment."""
        # Create temporary directory
        self.test_dir = tempfile.mkdtemp()
        self.fs = FileSystemOps(self.test_dir)
        
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)
    
    def test_file_operations(self):
        """Test basic file operations."""
        test_file = "test.txt"
        test_content = "Hello, World!"
        
        # Test write
        self.assertTrue(
            self.fs.write_file(test_file, test_content)
        )
        
        # Test read
        content = self.fs.read_file(test_file)
        self.assertEqual(content, test_content)
        
        # Test copy
        copy_file = "test_copy.txt"
        self.assertTrue(
            self.fs.copy_file(test_file, copy_file)
        )
        self.assertTrue(Path(self.test_dir, copy_file).exists())
        
        # Test move
        moved_file = "moved.txt"
        self.assertTrue(
            self.fs.move_file(copy_file, moved_file)
        )
        self.assertTrue(Path(self.test_dir, moved_file).exists())
        self.assertFalse(Path(self.test_dir, copy_file).exists())
        
        # Test delete
        self.assertTrue(self.fs.delete_file(test_file))
        self.assertFalse(Path(self.test_dir, test_file).exists())
    
    def test_directory_operations(self):
        """Test directory operations."""
        test_dir = "test_subdir"
        
        # Test create directory
        self.assertTrue(
            self.fs.create_directory(test_dir)
        )
        self.assertTrue(Path(self.test_dir, test_dir).is_dir())
        
        # Create test files
        for i in range(3):
            self.fs.write_file(
                f"{test_dir}/file{i}.txt",
                f"Content {i}"
            )
        
        # Test list directory
        contents = self.fs.list_directory(test_dir)
        self.assertEqual(len(contents), 3)
        self.assertTrue(
            all(item['type'] == 'file' for item in contents)
        )
    
    def test_permissions(self):
        """Test permission operations."""
        test_file = "test_perms.txt"
        self.fs.write_file(test_file, "Test content")
        
        # Test set permissions
        mode = 0o644
        self.assertTrue(
            self.fs.set_permissions(test_file, mode)
        )
        
        # Verify permissions
        metadata = self.fs.get_metadata(test_file)
        self.assertEqual(
            metadata['permissions'],
            '-rw-r--r--'
        )
    
    def test_metadata(self):
        """Test metadata retrieval."""
        test_file = "test_meta.txt"
        self.fs.write_file(test_file, "Test content")
        
        metadata = self.fs.get_metadata(test_file)
        
        self.assertEqual(metadata['type'], 'file')
        self.assertIsInstance(metadata['size'], int)
        self.assertIsInstance(metadata['created'], datetime)
        self.assertIsInstance(metadata['modified'], datetime)
        self.assertIsInstance(metadata['accessed'], datetime)
    
    def test_error_handling(self):
        """Test error handling."""
        # Test non-existent file
        with self.assertRaises(Exception):
            self.fs.read_file("nonexistent.txt")
        
        # Test invalid permissions
        with self.assertRaises(Exception):
            self.fs.set_permissions(
                "nonexistent.txt",
                0o644
            )
        
        # Test invalid path
        with self.assertRaises(Exception):
            self.fs.create_directory("../outside")
    
    def test_secure_delete(self):
        """Test secure file deletion."""
        test_file = "secure_test.txt"
        content = "Sensitive data"
        
        self.fs.write_file(test_file, content)
        self.assertTrue(
            self.fs.delete_file(test_file, secure=True)
        )
        self.assertFalse(Path(self.test_dir, test_file).exists())

if __name__ == '__main__':
    unittest.main() 