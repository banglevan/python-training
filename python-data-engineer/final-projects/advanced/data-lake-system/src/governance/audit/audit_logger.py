"""
Audit logging for data governance.
"""

from typing import Dict, Any, Optional
import json
import logging
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)

class AuditLogger:
    """Log audit events."""
    
    def __init__(
        self,
        storage_path: str,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize audit logger.
        
        Args:
            storage_path: Path to audit logs
            config: Logger configuration
        """
        self.storage_path = storage_path
        self.config = config or {}
    
    def log_access_attempt(
        self,
        user: str,
        action: str,
        resource: str,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Log access attempt.
        
        Args:
            user: User ID
            action: Attempted action
            resource: Target resource
            context: Additional context
            
        Returns:
            Event ID
        """
        try:
            event_id = str(uuid.uuid4())
            
            event = {
                'event_id': event_id,
                'timestamp': datetime.now().isoformat(),
                'event_type': 'access_attempt',
                'user': user,
                'action': action,
                'resource': resource,
                'context': context or {},
                'status': 'pending'
            }
            
            self._write_event(event)
            return event_id
            
        except Exception as e:
            logger.error(f"Failed to log access attempt: {e}")
            raise
    
    def log_access_granted(
        self,
        user: str,
        action: str,
        resource: str,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log granted access."""
        try:
            event = {
                'event_id': str(uuid.uuid4()),
                'timestamp': datetime.now().isoformat(),
                'event_type': 'access_granted',
                'user': user,
                'action': action,
                'resource': resource,
                'context': context or {}
            }
            
            self._write_event(event)
            
        except Exception as e:
            logger.error(f"Failed to log granted access: {e}")
            raise
    
    def log_access_denied(
        self,
        user: str,
        action: str,
        resource: str,
        policy_id: str,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log denied access."""
        try:
            event = {
                'event_id': str(uuid.uuid4()),
                'timestamp': datetime.now().isoformat(),
                'event_type': 'access_denied',
                'user': user,
                'action': action,
                'resource': resource,
                'policy_id': policy_id,
                'context': context or {}
            }
            
            self._write_event(event)
            
        except Exception as e:
            logger.error(f"Failed to log denied access: {e}")
            raise
    
    def log_data_access(
        self,
        user: str,
        action: str,
        resource: str,
        data_accessed: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log data access details."""
        try:
            event = {
                'event_id': str(uuid.uuid4()),
                'timestamp': datetime.now().isoformat(),
                'event_type': 'data_access',
                'user': user,
                'action': action,
                'resource': resource,
                'data_accessed': data_accessed,
                'context': context or {}
            }
            
            self._write_event(event)
            
        except Exception as e:
            logger.error(f"Failed to log data access: {e}")
            raise
    
    def _write_event(self, event: Dict[str, Any]) -> None:
        """Write event to storage."""
        try:
            # Add common fields
            event['version'] = self.config.get('version', '1.0')
            event['environment'] = self.config.get(
                'environment', 'production'
            )
            
            # Write to file
            filename = (
                f"{self.storage_path}/"
                f"{datetime.now().strftime('%Y%m%d')}.log"
            )
            
            with open(filename, 'a') as f:
                f.write(json.dumps(event) + '\n')
                
        except Exception as e:
            logger.error(f"Failed to write event: {e}")
            raise 