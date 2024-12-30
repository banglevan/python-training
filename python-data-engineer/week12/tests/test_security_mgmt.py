"""
Test security management operations.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import shutil
import os
import json
from datetime import datetime
from exercises.security_mgmt import SecurityManagement, AccessPolicy, Permission

class TestSecurityManagement(unittest.TestCase):
    """Test security management functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        cls.temp_dir = tempfile.mkdtemp()
        cls.config_path = os.path.join(cls.temp_dir, "config")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        shutil.rmtree(cls.temp_dir)
    
    def setUp(self):
        """Set up test case."""
        self.security = SecurityManagement(self.config_path)
    
    def test_policy_creation(self):
        """Test policy creation."""
        policy = AccessPolicy(
            name="test_policy",
            resources=["test/*"],
            permissions=[Permission.READ, Permission.WRITE],
            conditions={"role": "admin"}
        )
        
        # Create policy
        self.security.create_policy(policy)
        
        # Verify policy storage
        self.assertIn("test_policy", self.security.policies)
        
        stored_policy = self.security.policies["test_policy"]
        self.assertEqual(stored_policy["resources"], ["test/*"])
        self.assertEqual(
            stored_policy["permissions"],
            [Permission.READ.value, Permission.WRITE.value]
        )
    
    def test_access_check(self):
        """Test access checking."""
        # Create test policy
        policy = AccessPolicy(
            name="test_policy",
            resources=["test/*"],
            permissions=[Permission.READ],
            conditions={"role": "user"}
        )
        self.security.create_policy(policy)
        
        # Test allowed access
        self.assertTrue(
            self.security.check_access(
                "test_policy",
                "test/resource",
                Permission.READ,
                {"role": "user"}
            )
        )
        
        # Test denied access (wrong permission)
        self.assertFalse(
            self.security.check_access(
                "test_policy",
                "test/resource",
                Permission.WRITE,
                {"role": "user"}
            )
        )
        
        # Test denied access (wrong resource)
        self.assertFalse(
            self.security.check_access(
                "test_policy",
                "other/resource",
                Permission.READ,
                {"role": "user"}
            )
        )
        
        # Test denied access (wrong condition)
        self.assertFalse(
            self.security.check_access(
                "test_policy",
                "test/resource",
                Permission.READ,
                {"role": "guest"}
            )
        )
    
    def test_audit_logging(self):
        """Test audit logging."""
        # Log test event
        self.security.log_audit_event(
            "TEST",
            "Test message",
            {"detail": "test"}
        )
        
        # Verify audit log
        with open(self.security.audit_path, 'r') as f:
            log_entry = json.loads(f.readline())
        
        self.assertEqual(log_entry["type"], "TEST")
        self.assertEqual(log_entry["message"], "Test message")
        self.assertEqual(log_entry["details"], {"detail": "test"})
    
    def test_compliance_check(self):
        """Test compliance checking."""
        # Create invalid policy
        self.security.policies["invalid_policy"] = {
            "resources": ["test/*"],
            "permissions": ["invalid_permission"]
        }
        
        # Run compliance check
        issues = self.security.run_compliance_check()
        
        # Verify issues
        self.assertTrue(len(issues) > 0)
        self.assertTrue(
            any(i["type"] == "INVALID_PERMISSIONS" for i in issues)
        )
    
    def test_resource_matching(self):
        """Test resource pattern matching."""
        # Test exact match
        self.assertTrue(
            self.security._match_resource("test", "test")
        )
        
        # Test wildcard match
        self.assertTrue(
            self.security._match_resource("test/*", "test/resource")
        )
        
        # Test no match
        self.assertFalse(
            self.security._match_resource("test/*", "other/resource")
        )
    
    def test_condition_evaluation(self):
        """Test condition evaluation."""
        conditions = {
            "role": "admin",
            "env": "prod"
        }
        
        # Test matching context
        context = {
            "role": "admin",
            "env": "prod"
        }
        self.assertTrue(
            self.security._evaluate_conditions(conditions, context)
        )
        
        # Test non-matching context
        context = {
            "role": "user",
            "env": "prod"
        }
        self.assertFalse(
            self.security._evaluate_conditions(conditions, context)
        )

if __name__ == '__main__':
    unittest.main() 