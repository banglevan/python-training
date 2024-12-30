"""
Main application tests.
"""

import unittest
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient
import json

from main import create_app
from src.core.security import security_manager

class TestMainApplication(unittest.TestCase):
    """Test main application."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.app = create_app()
        self.client = TestClient(self.app)
        
        # Test user data
        self.test_user = {
            "username": "test_user",
            "email": "test@example.com",
            "password": "test_password",
            "role": "editor"
        }
        
        # Test dashboard data
        self.test_dashboard = {
            "name": "Test Dashboard",
            "layout": {"type": "grid", "columns": 2},
            "charts": [{
                "type": "line",
                "title": "Test Chart",
                "data_source": "test_source",
                "x_axis": {"field": "timestamp"},
                "y_axis": {"field": "value"}
            }]
        }
    
    def test_app_startup(self):
        """Test application startup."""
        with patch('src.core.database.DatabaseManager.init_db') as mock_init:
            # Execute startup event
            for event in self.app.router.on_startup:
                self.client.app.router.startup()
            
            # Verify
            mock_init.assert_called_once()
    
    def test_app_shutdown(self):
        """Test application shutdown."""
        with patch('src.core.database.DatabaseManager.close') as mock_close:
            # Execute shutdown event
            for event in self.app.router.on_shutdown:
                self.client.app.router.shutdown()
            
            # Verify
            mock_close.assert_called_once()
    
    @patch('src.core.database.DatabaseManager.get_db')
    def test_auth_endpoints(self, mock_db):
        """Test authentication endpoints."""
        # Setup mock database
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {
            'id': 1,
            'username': self.test_user['username'],
            'password_hash': security_manager.get_password_hash(
                self.test_user['password']
            ),
            'role': self.test_user['role']
        }
        mock_db.return_value.__enter__.return_value.execute.return_value = mock_cursor
        
        # Test login
        response = self.client.post(
            "/api/auth/login",
            json={
                "username": self.test_user['username'],
                "password": self.test_user['password']
            }
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertIn("access_token", response.json())
        
        # Store token for subsequent requests
        self.token = response.json()["access_token"]
    
    @patch('src.viz.dashboards.DashboardManager.create_dashboard')
    def test_dashboard_endpoints(self, mock_create):
        """Test dashboard endpoints."""
        # Setup
        mock_create.return_value = "test_dashboard_id"
        
        # Test dashboard creation
        response = self.client.post(
            "/api/viz/dashboards",
            headers={"Authorization": f"Bearer {self.token}"},
            json=self.test_dashboard
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["id"],
            "test_dashboard_id"
        )
    
    @patch('src.data.sources.SourceFactory.create_source')
    def test_data_source_endpoints(self, mock_source):
        """Test data source endpoints."""
        # Setup
        test_source = {
            "type": "postgres",
            "name": "test_source",
            "config": {
                "table": "test_table",
                "schema": "public"
            }
        }
        
        # Test source creation
        response = self.client.post(
            "/api/data/sources",
            headers={"Authorization": f"Bearer {self.token}"},
            json=test_source
        )
        
        self.assertEqual(response.status_code, 200)
    
    def test_unauthorized_access(self):
        """Test unauthorized access."""
        # Test without token
        response = self.client.post(
            "/api/viz/dashboards",
            json=self.test_dashboard
        )
        
        self.assertEqual(response.status_code, 401)
    
    def test_invalid_token(self):
        """Test invalid token access."""
        # Test with invalid token
        response = self.client.post(
            "/api/viz/dashboards",
            headers={"Authorization": "Bearer invalid_token"},
            json=self.test_dashboard
        )
        
        self.assertEqual(response.status_code, 401)
    
    @patch('src.core.database.DatabaseManager.get_db')
    def test_permission_checking(self, mock_db):
        """Test permission-based access control."""
        # Setup viewer user
        viewer_user = {
            "username": "viewer_user",
            "password": "test_password",
            "role": "viewer"
        }
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {
            'id': 2,
            'username': viewer_user['username'],
            'password_hash': security_manager.get_password_hash(
                viewer_user['password']
            ),
            'role': viewer_user['role']
        }
        mock_db.return_value.__enter__.return_value.execute.return_value = mock_cursor
        
        # Login as viewer
        response = self.client.post(
            "/api/auth/login",
            json={
                "username": viewer_user['username'],
                "password": viewer_user['password']
            }
        )
        
        viewer_token = response.json()["access_token"]
        
        # Test restricted endpoint
        response = self.client.post(
            "/api/data/sources",
            headers={"Authorization": f"Bearer {viewer_token}"},
            json={"type": "postgres", "name": "test"}
        )
        
        self.assertEqual(response.status_code, 403)

if __name__ == '__main__':
    unittest.main() 