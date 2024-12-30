"""
Security management exercises.
"""

from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
import json
import os
import re
from dataclasses import dataclass
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Permission(Enum):
    """Permission types."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"

@dataclass
class AccessPolicy:
    """Access policy definition."""
    name: str
    resources: List[str]
    permissions: List[Permission]
    conditions: Optional[Dict[str, Any]] = None

class SecurityManagement:
    """Security management operations."""
    
    def __init__(self, config_path: str = "config/"):
        """
        Initialize security manager.
        
        Args:
            config_path: Path to configuration
        """
        self.config_path = config_path
        os.makedirs(config_path, exist_ok=True)
        
        # Load policies
        self.policies = self.load_policies()
        
        # Initialize audit log
        self.audit_path = os.path.join(config_path, "audit.log")
    
    def create_policy(
        self,
        policy: AccessPolicy
    ) -> None:
        """
        Create access policy.
        
        Args:
            policy: Access policy
        """
        try:
            # Validate policy
            self._validate_policy(policy)
            
            # Add policy
            self.policies[policy.name] = {
                'resources': policy.resources,
                'permissions': [p.value for p in policy.permissions],
                'conditions': policy.conditions
            }
            
            # Save policies
            self._save_policies()
            
            # Audit
            self.log_audit_event(
                "CREATE_POLICY",
                f"Created policy: {policy.name}"
            )
            
            logger.info(f"Created policy: {policy.name}")
            
        except Exception as e:
            logger.error(f"Failed to create policy: {e}")
            raise
    
    def check_access(
        self,
        policy_name: str,
        resource: str,
        permission: Permission,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Check access permission.
        
        Args:
            policy_name: Policy name
            resource: Resource to access
            permission: Required permission
            context: Access context
        
        Returns:
            True if access is allowed
        """
        try:
            if policy_name not in self.policies:
                return False
            
            policy = self.policies[policy_name]
            
            # Check resource
            if not any(self._match_resource(r, resource) for r in policy['resources']):
                return False
            
            # Check permission
            if permission.value not in policy['permissions']:
                return False
            
            # Check conditions
            if policy['conditions'] and context:
                if not self._evaluate_conditions(policy['conditions'], context):
                    return False
            
            # Audit
            self.log_audit_event(
                "CHECK_ACCESS",
                f"Access check: {policy_name}, {resource}, {permission.value}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to check access: {e}")
            raise
    
    def log_audit_event(
        self,
        event_type: str,
        message: str,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log audit event.
        
        Args:
            event_type: Type of event
            message: Event message
            details: Additional details
        """
        try:
            event = {
                'timestamp': datetime.now().isoformat(),
                'type': event_type,
                'message': message,
                'details': details or {}
            }
            
            with open(self.audit_path, 'a') as f:
                f.write(json.dumps(event) + '\n')
            
        except Exception as e:
            logger.error(f"Failed to log audit event: {e}")
            raise
    
    def run_compliance_check(self) -> List[Dict[str, Any]]:
        """
        Run compliance checks.
        
        Returns:
            List of compliance issues
        """
        try:
            issues = []
            
            # Check policy definitions
            for name, policy in self.policies.items():
                # Check required fields
                if not all(k in policy for k in ['resources', 'permissions']):
                    issues.append({
                        'type': 'INVALID_POLICY',
                        'policy': name,
                        'message': 'Missing required fields'
                    })
                
                # Check permission values
                invalid_perms = [
                    p for p in policy['permissions']
                    if p not in [p.value for p in Permission]
                ]
                if invalid_perms:
                    issues.append({
                        'type': 'INVALID_PERMISSIONS',
                        'policy': name,
                        'permissions': invalid_perms
                    })
            
            # Check audit log
            if os.path.exists(self.audit_path):
                with open(self.audit_path, 'r') as f:
                    for line in f:
                        try:
                            json.loads(line)
                        except json.JSONDecodeError:
                            issues.append({
                                'type': 'INVALID_AUDIT_LOG',
                                'message': 'Invalid JSON format'
                            })
            
            # Log compliance check
            self.log_audit_event(
                "COMPLIANCE_CHECK",
                f"Found {len(issues)} issues"
            )
            
            return issues
            
        except Exception as e:
            logger.error(f"Failed to run compliance check: {e}")
            raise
    
    def _validate_policy(self, policy: AccessPolicy) -> None:
        """Validate policy definition."""
        if not policy.name or not policy.resources or not policy.permissions:
            raise ValueError("Invalid policy: missing required fields")
        
        if not all(isinstance(p, Permission) for p in policy.permissions):
            raise ValueError("Invalid policy: invalid permissions")
    
    def _match_resource(self, pattern: str, resource: str) -> bool:
        """Match resource pattern."""
        return bool(re.match(pattern.replace('*', '.*'), resource))
    
    def _evaluate_conditions(
        self,
        conditions: Dict[str, Any],
        context: Dict[str, Any]
    ) -> bool:
        """Evaluate access conditions."""
        for key, value in conditions.items():
            if key not in context or context[key] != value:
                return False
        return True
    
    def load_policies(self) -> Dict[str, Any]:
        """Load policies from file."""
        policy_path = os.path.join(self.config_path, "policies.json")
        
        if os.path.exists(policy_path):
            with open(policy_path, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_policies(self) -> None:
        """Save policies to file."""
        policy_path = os.path.join(self.config_path, "policies.json")
        
        with open(policy_path, 'w') as f:
            json.dump(self.policies, f, indent=2) 