"""
Test data encryption operations.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import shutil
import os
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from exercises.data_encryption import DataEncryption

class TestDataEncryption(unittest.TestCase):
    """Test encryption functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        cls.temp_dir = tempfile.mkdtemp()
        cls.key_store = os.path.join(cls.temp_dir, "keys")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        shutil.rmtree(cls.temp_dir)
    
    def setUp(self):
        """Set up test case."""
        self.encryption = DataEncryption(self.key_store)
    
    def test_symmetric_key_generation(self):
        """Test symmetric key generation."""
        key_id, key = self.encryption.generate_symmetric_key()
        
        # Verify key format
        self.assertTrue(isinstance(key_id, str))
        self.assertTrue(isinstance(key, bytes))
        
        # Verify key storage
        key_path = os.path.join(self.key_store, f"sym_{key_id}.key")
        self.assertTrue(os.path.exists(key_path))
    
    def test_asymmetric_key_generation(self):
        """Test asymmetric key generation."""
        key_id, private_key, public_key = self.encryption.generate_asymmetric_keys()
        
        # Verify key storage
        private_path = os.path.join(self.key_store, f"private_{key_id}.pem")
        public_path = os.path.join(self.key_store, f"public_{key_id}.pem")
        
        self.assertTrue(os.path.exists(private_path))
        self.assertTrue(os.path.exists(public_path))
    
    def test_symmetric_encryption(self):
        """Test symmetric encryption and decryption."""
        test_data = b"test data"
        
        # Encrypt
        key_id, encrypted = self.encryption.encrypt_symmetric(test_data)
        
        # Verify encryption
        self.assertNotEqual(encrypted, test_data)
        
        # Decrypt
        decrypted = self.encryption.decrypt_symmetric(encrypted, key_id)
        
        # Verify decryption
        self.assertEqual(decrypted, test_data)
    
    def test_key_rotation(self):
        """Test key rotation."""
        # Get initial key
        initial_key_id = self.encryption.current_key_id
        
        # Rotate key
        self.encryption.rotate_symmetric_key()
        
        # Verify key rotation
        self.assertNotEqual(
            self.encryption.current_key_id,
            initial_key_id
        )
    
    def test_automatic_rotation(self):
        """Test automatic key rotation."""
        # Create encryption manager with expired key
        with patch('exercises.data_encryption.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime.now()
            mock_datetime.fromisoformat.return_value = datetime.now() - timedelta(days=91)
            
            encryption = DataEncryption(self.key_store)
            
            # Verify key was rotated
            metadata_path = os.path.join(self.key_store, 'metadata.json')
            self.assertTrue(os.path.exists(metadata_path))
    
    def test_error_handling(self):
        """Test error handling."""
        # Test invalid key ID
        with self.assertRaises(Exception):
            self.encryption.decrypt_symmetric(
                b"test",
                "invalid_key_id"
            )
        
        # Test invalid data
        with self.assertRaises(Exception):
            self.encryption.decrypt_symmetric(
                b"invalid_data",
                self.encryption.current_key_id
            )

if __name__ == '__main__':
    unittest.main() 