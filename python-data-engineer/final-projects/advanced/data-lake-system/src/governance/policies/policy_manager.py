"""
Data governance policy management.
"""

from typing import Dict, Any, List, Optional
import json
import logging
from datetime import datetime
from ..audit.audit_logger import AuditLogger

logger = logging.getLogger(__name__)

class PolicyManager:
    """Manage data governance policies."""
    
    def __init__(
        self,
        config_path: str,
        audit_logger: AuditLogger
    ):
        """
        Initialize policy manager.
        
        Args:
            config_path: Path to policy config
            audit_logger: Audit logger instance
        """
        self.config_path = config_path
        self.audit_logger = audit_logger
        self.policies = self._load_policies()
    
    def _load_policies(self) -> Dict[str, Any]:
        """Load policies from config."""
        try:
            with open(self.config_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load policies: {e}")
            raise
    
    def validate_access(
        self,
        user: str,
        action: str,
        resource: str,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Validate access request.
        
        Args:
            user: User ID
            action: Requested action
            resource: Target resource
            context: Additional context
            
        Returns:
            True if access granted
        """
        try:
            logger.info(
                f"Validating access: {user} -> {action} -> "
                f"{resource}"
            )
            
            # Get applicable policies
            policies = self._get_applicable_policies(
                user, action, resource
            )
            
            if not policies:
                logger.warning("No applicable policies found")
                return False
            
            # Check each policy
            for policy in policies:
                if not self._check_policy(
                    policy, user, action, resource, context
                ):
                    self.audit_logger.log_access_denied(
                        user, action, resource, policy['id']
                    )
                    return False
            
            # Access granted
            self.audit_logger.log_access_granted(
                user, action, resource
            )
            return True
            
        except Exception as e:
            logger.error(f"Access validation failed: {e}")
            raise
    
    def _get_applicable_policies(
        self,
        user: str,
        action: str,
        resource: str
    ) -> List[Dict[str, Any]]:
        """Get policies applicable to request."""
        try:
            applicable = []
            
            for policy in self.policies['policies']:
                # Check user match
                if not self._match_user(user, policy):
                    continue
                
                # Check action match
                if not self._match_action(action, policy):
                    continue
                
                # Check resource match
                if not self._match_resource(resource, policy):
                    continue
                
                applicable.append(policy)
            
            return applicable
            
        except Exception as e:
            logger.error(
                f"Failed to get applicable policies: {e}"
            )
            raise
    
    def _check_policy(
        self,
        policy: Dict[str, Any],
        user: str,
        action: str,
        resource: str,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Check if policy allows access."""
        try:
            # Check conditions
            conditions = policy.get('conditions', [])
            for condition in conditions:
                if not self._evaluate_condition(
                    condition, user, action, resource, context
                ):
                    return False
            
            # Check restrictions
            restrictions = policy.get('restrictions', [])
            for restriction in restrictions:
                if self._evaluate_restriction(
                    restriction, user, action, resource, context
                ):
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Policy check failed: {e}")
            raise
    
    def _evaluate_condition(
        self,
        condition: Dict[str, Any],
        user: str,
        action: str,
        resource: str,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Evaluate policy condition."""
        try:
            condition_type = condition['type']
            
            if condition_type == 'time_window':
                return self._check_time_window(condition)
            elif condition_type == 'ip_range':
                return self._check_ip_range(
                    condition, context
                )
            elif condition_type == 'attribute':
                return self._check_attribute(
                    condition, context
                )
            else:
                logger.warning(
                    f"Unknown condition type: {condition_type}"
                )
                return False
                
        except Exception as e:
            logger.error(
                f"Condition evaluation failed: {e}"
            )
            raise 