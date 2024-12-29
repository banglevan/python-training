"""
NFS Client Tests
--------------

Tests NFS client operations with:
1. Mount operations
2. Connection handling
3. Space management
4. Error scenarios
"""

import unittest
from unittest.mock import patch, Mock
import subprocess
from pathlib import Path
import socket
from nfs_client import NFSClient

class TestNFSClient(unittest.TestCase):
    """Test cases for NFSClient."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            'server': 'test.server',
            'remote_path': '/shared',
            'mount_point': '/mnt/test',
            'options': {
                'rw': None,
                'sync': None,
                'vers': '4'
            }
        }
        self.client = NFSClient(**self.config)
    
    @patch('subprocess.run')
    def test_mount(self, mock_run):
        """Test mount operation."""
        # Mock successful mount
        mock_run.return_value = Mock(
            returncode=0,
            stdout="",
            stderr=""
        )
        
        # Test mount
        self.assertTrue(self.client.mount())
        
        # Verify command
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        self.assertEqual(cmd[0], 'mount')
        self.assertEqual(cmd[1], '-t')
        self.assertEqual(cmd[2], 'nfs')
    
    @patch('subprocess.run')
    def test_unmount(self, mock_run):
        """Test unmount operation."""
        # Mock successful unmount
        mock_run.return_value = Mock(
            returncode=0,
            stdout="",
            stderr=""
        )
        
        # Test unmount
        self.assertTrue(self.client.unmount())
        
        # Verify command
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        self.assertEqual(cmd[0], 'umount')
    
    @patch('builtins.open')
    def test_is_mounted(self, mock_open):
        """Test mount check."""
        # Mock /proc/mounts content
        mock_open.return_value.__enter__.return_value.read.return_value = \
            f"... {self.config['mount_point']} ..."
        
        self.assertTrue(self.client.is_mounted())
    
    @patch('subprocess.run')
    def test_mount_stats(self, mock_run):
        """Test mount statistics."""
        # Mock nfsstat output
        mock_run.return_value = Mock(
            returncode=0,
            stdout="age: 5\nvers: 4\nproto: tcp",
            stderr=""
        )
        
        # Test stats retrieval
        stats = self.client.get_mount_stats()
        self.assertIsInstance(stats, dict)
        self.assertEqual(stats['age'], '5')
        self.assertEqual(stats['vers'], '4')
    
    @patch('socket.create_connection')
    def test_connectivity(self, mock_connect):
        """Test server connectivity check."""
        # Test successful connection
        mock_connect.return_value = Mock()
        self.assertTrue(self.client.check_connectivity())
        
        # Test failed connection
        mock_connect.side_effect = socket.error
        self.assertFalse(self.client.check_connectivity())
    
    @patch('os.statvfs')
    def test_space_info(self, mock_statvfs):
        """Test space information retrieval."""
        # Mock statvfs result
        mock_stat = Mock()
        mock_stat.f_blocks = 1000
        mock_stat.f_bfree = 500
        mock_stat.f_bavail = 400
        mock_stat.f_frsize = 4096
        mock_statvfs.return_value = mock_stat
        
        # Test space info
        space = self.client.get_space_info()
        self.assertIn('total', space)
        self.assertIn('free', space)
        self.assertIn('available', space)
    
    def test_context_manager(self):
        """Test context manager functionality."""
        with patch.object(self.client, 'mount') as mock_mount, \
             patch.object(self.client, 'unmount') as mock_unmount:
            
            with self.client:
                mock_mount.assert_called_once()
            
            mock_unmount.assert_called_once()
    
    @patch('subprocess.run')
    def test_mount_error(self, mock_run):
        """Test mount error handling."""
        # Mock mount failure
        mock_run.side_effect = subprocess.CalledProcessError(
            1, 'mount', stderr="Mount failed"
        )
        
        with self.assertRaises(subprocess.CalledProcessError):
            self.client.mount()
    
    @patch('socket.create_connection')
    @patch.object(NFSClient, 'mount')
    @patch.object(NFSClient, 'unmount')
    def test_remount(
        self,
        mock_unmount,
        mock_mount,
        mock_connect
    ):
        """Test remount functionality."""
        # Mock lost connection
        mock_connect.side_effect = socket.error
        
        # Test remount
        self.client.remount_if_needed()
        
        mock_unmount.assert_called_once()
        mock_mount.assert_called_once()

if __name__ == '__main__':
    unittest.main() 