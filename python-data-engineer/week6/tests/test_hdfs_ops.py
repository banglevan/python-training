"""
HDFS Operations Tests
------------------

Tests HDFS operations with:
1. File operations
2. Block management
3. Cluster monitoring
4. Error handling
"""

import unittest
from unittest.mock import Mock, patch
import pyarrow as pa
import pyarrow.hdfs as hdfs
from pathlib import Path
import json
from datetime import datetime
from hdfs_ops import HDFSOperations

class TestHDFSOperations(unittest.TestCase):
    """Test cases for HDFS operations."""
    
    def setUp(self):
        """Set up test environment."""
        self.mock_conn = Mock()
        with patch('pyarrow.hdfs.connect', return_value=self.mock_conn):
            self.hdfs = HDFSOperations(
                host='test-host',
                port=9000
            )
    
    def test_upload_file(self):
        """Test file upload to HDFS."""
        # Create mock file and HDFS file objects
        mock_local_file = Mock()
        mock_hdfs_file = Mock()
        
        # Mock file operations
        with patch('pathlib.Path.open',
                  return_value=mock_local_file), \
             patch.object(self.mock_conn, 'open',
                         return_value=mock_hdfs_file), \
             patch('pathlib.Path.exists', return_value=True):
            
            # Mock read/write operations
            mock_local_file.__enter__.return_value = mock_local_file
            mock_hdfs_file.__enter__.return_value = mock_hdfs_file
            mock_local_file.read.side_effect = [b'data', b'']
            
            # Test upload
            result = self.hdfs.upload_file(
                'local.txt',
                '/test/remote.txt',
                chunk_size=1024
            )
            
            self.assertTrue(result)
            mock_hdfs_file.write.assert_called_with(b'data')
    
    def test_download_file(self):
        """Test file download from HDFS."""
        # Create mock HDFS and local file objects
        mock_hdfs_file = Mock()
        mock_local_file = Mock()
        
        # Mock file operations
        with patch('pathlib.Path.open',
                  return_value=mock_local_file), \
             patch.object(self.mock_conn, 'open',
                         return_value=mock_hdfs_file), \
             patch('pathlib.Path.parent.mkdir'):
            
            # Mock read/write operations
            mock_hdfs_file.__enter__.return_value = mock_hdfs_file
            mock_local_file.__enter__.return_value = mock_local_file
            mock_hdfs_file.read.side_effect = [b'data', b'']
            
            # Test download
            result = self.hdfs.download_file(
                '/test/remote.txt',
                'local.txt',
                chunk_size=1024
            )
            
            self.assertTrue(result)
            mock_local_file.write.assert_called_with(b'data')
    
    def test_read_file(self):
        """Test reading file content."""
        mock_file = Mock()
        mock_file.__enter__.return_value = mock_file
        mock_file.read.return_value = b'test content'
        
        with patch.object(self.mock_conn, 'open',
                         return_value=mock_file):
            content = self.hdfs.read_file('/test/file.txt')
            self.assertEqual(content, 'test content')
    
    def test_write_file(self):
        """Test writing file content."""
        mock_file = Mock()
        mock_file.__enter__.return_value = mock_file
        
        with patch.object(self.mock_conn, 'open',
                         return_value=mock_file):
            result = self.hdfs.write_file(
                '/test/file.txt',
                'test content'
            )
            self.assertTrue(result)
            mock_file.write.assert_called_once()
    
    def test_delete(self):
        """Test file deletion."""
        self.hdfs.delete('/test/file.txt')
        self.mock_conn.delete.assert_called_with(
            '/test/file.txt',
            recursive=False
        )
    
    def test_list_directory(self):
        """Test directory listing."""
        mock_info = [{
            'name': '/test/file.txt',
            'kind': 'file',
            'size': 1024,
            'mtime': datetime.now().timestamp(),
            'owner': 'hadoop',
            'group': 'hadoop',
            'mode': 0o644,
            'replication': 3,
            'block_size': 128 * 1024 * 1024
        }]
        
        self.mock_conn.ls.return_value = mock_info
        result = self.hdfs.list_directory('/test')
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], '/test/file.txt')
        self.assertEqual(result[0]['type'], 'file')
    
    def test_get_block_locations(self):
        """Test block location retrieval."""
        mock_blocks = [{
            'offset': 0,
            'length': 1024,
            'hosts': ['datanode1', 'datanode2']
        }]
        
        self.mock_conn.get_block_locations.return_value = mock_blocks
        result = self.hdfs.get_block_locations('/test/file.txt')
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['hosts'],
                        ['datanode1', 'datanode2'])
    
    def test_set_replication(self):
        """Test replication factor setting."""
        result = self.hdfs.set_replication(
            '/test/file.txt',
            3
        )
        self.assertTrue(result)
        self.mock_conn.set_replication.assert_called_with(
            '/test/file.txt',
            3
        )
    
    def test_get_cluster_status(self):
        """Test cluster status retrieval."""
        mock_status = {
            'capacity': 1000,
            'used': 200,
            'remaining': 800,
            'live_datanodes': ['node1', 'node2'],
            'dead_datanodes': []
        }
        
        self.mock_conn.get_capacity_information.return_value = \
            mock_status
        
        result = self.hdfs.get_cluster_status()
        
        self.assertEqual(result['capacity'], 1000)
        self.assertEqual(result['used'], 200)
        self.assertEqual(result['used_percent'], 20.0)
    
    def test_error_handling(self):
        """Test error handling."""
        # Test file not found
        self.mock_conn.open.side_effect = \
            FileNotFoundError("File not found")
        
        with self.assertRaises(FileNotFoundError):
            self.hdfs.read_file('/nonexistent/file.txt')
        
        # Test permission error
        self.mock_conn.open.side_effect = \
            PermissionError("Permission denied")
        
        with self.assertRaises(PermissionError):
            self.hdfs.write_file(
                '/protected/file.txt',
                'content'
            )
    
    def tearDown(self):
        """Clean up test environment."""
        self.hdfs.close()

if __name__ == '__main__':
    unittest.main() 