"""
Core functionality tests.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import yaml
from datetime import datetime, timedelta

from src.core.config import Config
from src.core.database import DatabaseManager
from src.core.security import SecurityManager

class TestConfig(unittest.TestCase):
    """Test configuration management."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_config = {
            'database': {
                'postgres': {
                    'host': 'localhost',
                    'port': 5432
                }
            },
            'security': {
                'secret_key': 'test-key'
            }
        }
    
    @patch('builtins.open')
    def test_load_config(self, mock_open):
        """Test configuration loading."""
        # Setup
        mock_open.return_value.__enter__.return_value = Mock()
        yaml.safe_load = Mock(return_value=self.test_config)
        
        # Execute
        config = Config('dummy_path')
        
        # Verify
        self.assertEqual(
            config.get('database.postgres.host'),
            'localhost'
        )
        self.assertEqual(
            config.get('database.postgres.port'),
            5432
        )
    
    @patch('builtins.open')
    def test_validate_config(self, mock_open):
        """Test configuration validation."""
        # Setup
        mock_open.return_value.__enter__.return_value = Mock()
        yaml.safe_load = Mock(return_value=self.test_config)
        
        # Execute
        config = Config('dummy_path')
        
        # Verify
        self.assertFalse(config.validate())  # Missing required fields

class TestDatabaseManager(unittest.TestCase):
    """Test database management."""
    
    @patch('psycopg2.connect')
    @patch('pymongo.MongoClient')
    @patch('redis.Redis')
    def test_init_connections(self, mock_redis, mock_mongo, mock_pg):
        """Test database connections initialization."""
        # Setup
        db_manager = DatabaseManager()
        
        # Verify
        mock_pg.assert_called_once()
        mock_mongo.assert_called_once()
        mock_redis.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_get_db(self, mock_connect):
        """Test database session management."""
        # Setup
        db_manager = DatabaseManager()
        mock_db = Mock()
        mock_connect.return_value = mock_db
        
        # Execute
        with db_manager.get_db() as db:
            pass
        
        # Verify
        mock_db.close.assert_called_once()

class TestSecurityManager(unittest.TestCase):
    """Test security management."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.security = SecurityManager()
    
    def test_password_hash(self):
        """Test password hashing."""
        # Execute
        password = "test_password"
        hashed = self.security.get_password_hash(password)
        
        # Verify
        self.assertTrue(
            self.security.verify_password(password, hashed)
        )
    
    def test_create_access_token(self):
        """Test JWT token creation."""
        # Execute
        token = self.security.create_access_token(
            {"sub": "test_user"}
        )
        
        # Verify
        self.assertIsInstance(token, str)
    
    def test_check_permissions(self):
        """Test permission checking."""
        # Execute and verify
        self.assertTrue(
            self.security.check_permissions("viewer", "admin")
        )
        self.assertFalse(
            self.security.check_permissions("admin", "viewer")
        )

if __name__ == '__main__':
    unittest.main() 