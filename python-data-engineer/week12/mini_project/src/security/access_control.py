"""
Access control management.
"""

from typing import Dict, Any, Optional, List, Union
import logging
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AccessControl:
    """Access control management."""
    
    def __init__(self):
        """Initialize manager."""
        self.policies = {}
        self.row_filters = {}
        self.column_masks = {}
    
    def create_policy(
        self,
        name: str,
        resource_type: str,
        actions: List[str],
        conditions: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Create access policy.
        
        Args:
            name: Policy name
            resource_type: Resource type
            actions: Allowed actions
            conditions: Access conditions
        """
        try:
            if name in self.policies:
                raise ValueError(f"Policy already exists: {name}")
            
            self.policies[name] = {
                'resource_type': resource_type,
                'actions': actions,
                'conditions': conditions or {},
                'created_at': datetime.now().isoformat()
            }
            
            logger.info(f"Created policy: {name}")
            
        except Exception as e:
            logger.error(f"Failed to create policy: {e}")
            raise
    
    def add_row_filter(
        self,
        table: str,
        filter_expr: str,
        roles: List[str]
    ) -> None:
        """
        Add row-level filter.
        
        Args:
            table: Table name
            filter_expr: Filter expression
            roles: Applicable roles
        """
        try:
            if table not in self.row_filters:
                self.row_filters[table] = []
            
            self.row_filters[table].append({
                'filter_expr': filter_expr,
                'roles': roles,
                'created_at': datetime.now().isoformat()
            })
            
            logger.info(f"Added row filter for table: {table}")
            
        except Exception as e:
            logger.error(f"Failed to add row filter: {e}")
            raise
    
    def add_column_mask(
        self,
        table: str,
        column: str,
        mask_type: str,
        roles: List[str]
    ) -> None:
        """
        Add column masking.
        
        Args:
            table: Table name
            column: Column name
            mask_type: Masking type
            roles: Applicable roles
        """
        try:
            if table not in self.column_masks:
                self.column_masks[table] = {}
            
            if column not in self.column_masks[table]:
                self.column_masks[table][column] = []
            
            self.column_masks[table][column].append({
                'mask_type': mask_type,
                'roles': roles,
                'created_at': datetime.now().isoformat()
            })
            
            logger.info(f"Added column mask: {table}.{column}")
            
        except Exception as e:
            logger.error(f"Failed to add column mask: {e}")
            raise
    
    def check_access(
        self,
        user_roles: List[str],
        resource_type: str,
        action: str,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Check access permission.
        
        Args:
            user_roles: User roles
            resource_type: Resource type
            action: Requested action
            context: Access context
        
        Returns:
            Whether access is allowed
        """
        try:
            for policy in self.policies.values():
                if policy['resource_type'] != resource_type:
                    continue
                
                if action not in policy['actions']:
                    continue
                
                # Check conditions
                conditions_met = True
                for key, value in policy['conditions'].items():
                    if context and key in context:
                        if context[key] != value:
                            conditions_met = False
                            break
                    else:
                        conditions_met = False
                        break
                
                if conditions_met:
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to check access: {e}")
            raise
    
    def get_row_filters(
        self,
        table: str,
        user_roles: List[str]
    ) -> List[str]:
        """
        Get applicable row filters.
        
        Args:
            table: Table name
            user_roles: User roles
        
        Returns:
            List of filter expressions
        """
        try:
            if table not in self.row_filters:
                return []
            
            filters = []
            for filter_def in self.row_filters[table]:
                if any(role in user_roles for role in filter_def['roles']):
                    filters.append(filter_def['filter_expr'])
            
            return filters
            
        except Exception as e:
            logger.error(f"Failed to get row filters: {e}")
            raise
    
    def get_column_masks(
        self,
        table: str,
        user_roles: List[str]
    ) -> Dict[str, str]:
        """
        Get applicable column masks.
        
        Args:
            table: Table name
            user_roles: User roles
        
        Returns:
            Dictionary of column masks
        """
        try:
            if table not in self.column_masks:
                return {}
            
            masks = {}
            for column, mask_defs in self.column_masks[table].items():
                for mask_def in mask_defs:
                    if any(role in user_roles for role in mask_def['roles']):
                        masks[column] = mask_def['mask_type']
                        break
            
            return masks
            
        except Exception as e:
            logger.error(f"Failed to get column masks: {e}")
            raise 