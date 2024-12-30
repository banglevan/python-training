"""
Ceph Storage Tests
----------------

Tests Ceph operations across:
1. Object Storage (RADOS)
2. Block Storage (RBD)
3. File System (CephFS)
4. Cluster Management
"""

import unittest
from unittest.mock import Mock, patch, call
import rados
import rbd
import json
from pathlib import Path
from datetime import datetime
import subprocess
from ceph_storage import CephStorage

class TestCephStorage(unittest.TestCase):
    """Test cases for Ceph storage operations."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock Ceph cluster
        self.mock_cluster = Mock(spec=rados.Rados)
        self.mock_ioctx = Mock(spec=rados.Ioctx)
        self.mock_rbd = Mock(spec=rbd.RBD)
        
        # Setup patches
        self.patches = [
            patch('rados.Rados', return_value=self.mock_cluster),
            patch('rbd.RBD', return_value=self.mock_rbd)
        ]
        
        # Start patches
        for p in self.patches:
            p.start()
        
        # Mock cluster connection
        self.mock_cluster.connect = Mock()
        self.mock_cluster.open_ioctx = Mock(
            return_value=self.mock_ioctx
        )
        
        # Initialize CephStorage
        self.ceph = CephStorage(
            conf_file='/etc/ceph/ceph.conf',
            pool_name='test-pool'
        )
    
    def test_pool_operations(self):
        """Test pool management operations."""
        # Test list pools
        mock_pools = ['pool1', 'pool2']
        self.mock_cluster.list_pools.return_value = mock_pools
        
        pools = self.ceph.list_pools()
        self.assertEqual(pools, mock_pools)
        
        # Test create pool
        result = self.ceph.create_pool('new-pool', 32, 32)
        self.assertTrue(result)
        self.mock_cluster.create_pool.assert_called_with(
            'new-pool', 32, 32
        )
        
        # Test delete pool
        result = self.ceph.delete_pool('old-pool')
        self.assertTrue(result)
        self.mock_cluster.delete_pool.assert_called_with(
            'old-pool'
        )
    
    def test_object_operations(self):
        """Test RADOS object operations."""
        # Test write object
        result = self.ceph.write_object(
            'test-obj',
            'test data'
        )
        self.assertTrue(result)
        self.mock_ioctx.write_full.assert_called_with(
            'test-obj',
            b'test data'
        )
        
        # Test read object
        self.mock_ioctx.read.return_value = b'test data'
        data = self.ceph.read_object('test-obj')
        self.assertEqual(data, 'test data')
        
        # Test delete object
        result = self.ceph.delete_object('test-obj')
        self.assertTrue(result)
        self.mock_ioctx.remove_object.assert_called_with(
            'test-obj'
        )
        
        # Test list objects
        mock_objects = [Mock(key='obj1'), Mock(key='obj2')]
        self.mock_ioctx.list_objects.return_value = mock_objects
        objects = self.ceph.list_objects()
        self.assertEqual(len(objects), 2)
    
    def test_block_operations(self):
        """Test RBD block operations."""
        # Test create image
        result = self.ceph.create_image(
            'test-img',
            1024 * 1024,  # 1MB
            features=None
        )
        self.assertTrue(result)
        self.mock_rbd.create.assert_called_with(
            self.mock_ioctx,
            'test-img',
            1024 * 1024,
            features=None,
            old_format=False
        )
        
        # Test remove image
        result = self.ceph.remove_image('test-img')
        self.assertTrue(result)
        self.mock_rbd.remove.assert_called_with(
            self.mock_ioctx,
            'test-img'
        )
    
    def test_snapshot_operations(self):
        """Test snapshot operations."""
        mock_image = Mock(spec=rbd.Image)
        
        with patch('rbd.Image',
                  return_value=mock_image) as mock_image_cls:
            # Test create snapshot
            result = self.ceph.create_snapshot(
                'test-img',
                'snap1'
            )
            self.assertTrue(result)
            mock_image.create_snap.assert_called_with('snap1')
            
            # Test remove snapshot
            result = self.ceph.remove_snapshot(
                'test-img',
                'snap1'
            )
            self.assertTrue(result)
            mock_image.remove_snap.assert_called_with('snap1')
    
    def test_clone_operations(self):
        """Test image cloning operations."""
        mock_parent = Mock(spec=rbd.Image)
        
        with patch('rbd.Image',
                  return_value=mock_parent) as mock_image_cls:
            result = self.ceph.clone_image(
                'parent-img',
                'snap1',
                'clone-img'
            )
            self.assertTrue(result)
            
            mock_parent.protect_snap.assert_called_with('snap1')
            self.mock_rbd.clone.assert_called_with(
                self.mock_ioctx,
                'parent-img',
                'snap1',
                self.mock_ioctx,
                'clone-img'
            )
    
    @patch('subprocess.run')
    def test_filesystem_operations(self, mock_run):
        """Test CephFS operations."""
        # Test mount
        mock_run.return_value = Mock(returncode=0)
        
        result = self.ceph.mount_fs(
            '/mnt/cephfs',
            'mon1',
            'admin',
            'cephfs'
        )
        self.assertTrue(result)
        
        # Verify mount command
        mock_run.assert_called_with(
            [
                'mount',
                '-t', 'ceph',
                'mon1:/', '/mnt/cephfs',
                '-o',
                'name=admin,fs=cephfs'
            ],
            check=True
        )
        
        # Test unmount
        result = self.ceph.unmount_fs('/mnt/cephfs')
        self.assertTrue(result)
        mock_run.assert_called_with(
            ['umount', '/mnt/cephfs'],
            check=True
        )
    
    @patch('subprocess.run')
    def test_cluster_status(self, mock_run):
        """Test cluster status retrieval."""
        mock_status = {
            'health': {'status': 'HEALTH_OK'},
            'monmap': {'mons': ['mon1', 'mon2']},
            'osdmap': {'osds': ['osd1', 'osd2']},
            'pgmap': {'pgs': 100},
            'fsmap': {'filesystems': ['cephfs']}
        }
        
        mock_run.return_value = Mock(
            stdout=json.dumps(mock_status),
            returncode=0
        )
        
        status = self.ceph.get_cluster_status()
        
        self.assertEqual(
            status['health'],
            'HEALTH_OK'
        )
        self.assertIn('timestamp', status)
    
    def test_error_handling(self):
        """Test error handling scenarios."""
        # Test connection error
        self.mock_cluster.connect.side_effect = \
            rados.Error("Connection failed")
        
        with self.assertRaises(rados.Error):
            CephStorage()
        
        # Test write error
        self.mock_ioctx.write_full.side_effect = \
            rados.Error("Write failed")
        
        with self.assertRaises(rados.Error):
            self.ceph.write_object('test-obj', 'data')
        
        # Test mount error
        with patch('subprocess.run',
                  side_effect=subprocess.CalledProcessError(
                      1, 'mount'
                  )):
            with self.assertRaises(subprocess.CalledProcessError):
                self.ceph.mount_fs('/mnt/cephfs', 'mon1')
    
    def tearDown(self):
        """Clean up test environment."""
        # Stop patches
        for p in self.patches:
            p.stop()
        
        # Close Ceph connection
        self.ceph.close()

if __name__ == '__main__':
    unittest.main() 