"""
Data Security Tests
----------------

Tests security features with:
1. Encryption operations
2. Access control
3. Audit logging
"""

import unittest
from unittest.mock import Mock, patch, mock_open
import os
from pathlib import Path
import json
import jwt
import tempfile
import shutil
import sqlite3
from datetime import datetime, timedelta
from data_security import DataSecurity

class TestDataSecurity(unittest.TestCase):
    """Test cases for data security."""
    
    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.security = DataSecurity(
            self.test_dir,
            jwt_secret='test-secret'
        )
        
        # Create test data
        self.test_data = b"Sensitive data"
        self.test_file = Path(self.test_dir) / "secret.txt"
        with open(self.test_file, "wb") as f:
            f.write(self.test_data)
    
    def test_encrypt_decrypt_file(self):
        """Test file encryption and decryption."""
        # Encrypt file
        encrypt_result = self.security.encrypt_file(
            self.test_file,
            metadata={'purpose': 'test'}
        )
        
        self.assertTrue(
            Path(encrypt_result['encrypted_path']).exists()
        )
        self.assertNotEqual(
            Path(encrypt_result['encrypted_path']).read_bytes(),
            self.test_data
        )
        
        # Decrypt file
        decrypt_result = self.security.decrypt_file(
            encrypt_result['encrypted_path'],
            encrypt_result['key_id']
        )
        
        with open(decrypt_result['decrypted_path'], 'rb') as f:
            decrypted_data = f.read()
        
        self.assertEqual(decrypted_data, self.test_data)
    
    def test_user_management(self):
        """Test user creation and authentication."""
        # Create user
        user = self.security.create_user(
            'testuser',
            'password123',
            ['admin', 'user']
        )
        
        self.assertIsNotNone(user['user_id'])
        self.assertEqual(user['username'], 'testuser')
        self.assertEqual(user['roles'], ['admin', 'user'])
        
        # Authenticate user
        token = self.security.authenticate(
            'testuser',
            'password123'
        )
        
        # Verify token
        decoded = jwt.decode(
            token,
            self.security.jwt_secret,
            algorithms=['HS256']
        )
        
        self.assertEqual(decoded['username'], 'testuser')
        self.assertEqual(decoded['roles'], ['admin', 'user'])
    
    def test_access_control(self):
        """Test role-based access control."""
        # Create users with different roles
        admin = self.security.create_user(
            'admin',
            'admin123',
            ['admin']
        )
        user = self.security.create_user(
            'user',
            'user123',
            ['user']
        )
        
        # Get tokens
        admin_token = self.security.authenticate(
            'admin',
            'admin123'
        )
        user_token = self.security.authenticate(
            'user',
            'user123'
        )
        
        # Test admin access
        self.assertTrue(
            self.security.check_permission(
                admin_token,
                'admin_action'
            )
        )
        
        # Test user access
        self.assertFalse(
            self.security.check_permission(
                user_token,
                'admin_action'
            )
        )
    
    def test_audit_logging(self):
        """Test audit logging functionality."""
        # Create some events
        self.security.log_audit_event(
            'test_event',
            {'action': 'test'}
        )
        
        # Wait for async processing
        self.security.audit_queue.join()
        
        # Get logs
        logs = self.security.get_audit_logs(
            event_type='test_event',
            limit=1
        )
        
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]['event_type'], 'test_event')
        self.assertEqual(
            json.loads(logs[0]['event_data'])['action'],
            'test'
        )
    
    def test_key_management(self):
        """Test encryption key management."""
        # Generate key
        key_id = self.security._generate_key_id()
        key1 = self.security._get_or_create_key(key_id)
        
        # Retrieve same key
        key2 = self.security._get_key(key_id)
        
        self.assertEqual(key1, key2)
        
        # Test non-existent key
        self.assertIsNone(
            self.security._get_key('nonexistent')
        )
    
    def test_password_hashing(self):
        """Test password hashing and verification."""
        password = "test_password"
        
        # Hash password
        hashed = self.security._hash_password(password)
        
        # Verify correct password
        self.assertTrue(
            self.security._verify_password(
                password,
                hashed
            )
        )
        
        # Verify incorrect password
        self.assertFalse(
            self.security._verify_password(
                'wrong_password',
                hashed
            )
        )
    
    def test_error_handling(self):
        """Test error handling."""
        # Test non-existent file
        with self.assertRaises(FileNotFoundError):
            self.security.encrypt_file("nonexistent.txt")
        
        # Test invalid key
        with self.assertRaises(ValueError):
            self.security.decrypt_file(
                self.test_file,
                'invalid_key'
            )
        
        # Test invalid token
        with self.assertRaises(jwt.InvalidTokenError):
            self.security.check_permission(
                'invalid_token',
                'action'
            )
    
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)

if __name__ == '__main__':
    unittest.main() 