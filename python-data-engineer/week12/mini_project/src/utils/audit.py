"""
Audit logging utilities.
"""

from typing import Dict, Any, Optional
import logging
from datetime import datetime
import json
from src.init.platform import platform

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AuditLogger:
    """Audit logging management."""
    
    @staticmethod
    def log_event(
        user_id: str,
        action: str,
        resource_type: str,
        resource_id: str,
        status: str,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log audit event.
        
        Args:
            user_id: User ID
            action: Action performed
            resource_type: Resource type
            resource_id: Resource ID
            status: Event status
            details: Additional details
        """
        try:
            spark = platform.get_component('spark')
            
            # Convert details to JSON
            details_json = json.dumps(details) if details else None
            
            # Insert audit log
            spark.sql(f"""
                INSERT INTO audit_log VALUES (
                    '{datetime.now().isoformat()}',
                    '{user_id}',
                    '{action}',
                    '{resource_type}',
                    '{resource_id}',
                    '{status}',
                    '{details_json}'
                )
            """)
            
            logger.info(f"Logged audit event: {action} by {user_id}")
            
        except Exception as e:
            logger.error(f"Failed to log audit event: {e}") 