"""
Sync management package.

This package provides components for data synchronization:
- Manager: Orchestrates sync operations
- Conflict: Handles data conflicts
- State: Manages entity states
"""

from typing import Dict, Any, Optional
import logging

from .manager import SyncManager
from .conflict import ConflictResolver
from .state import StateManager

logger = logging.getLogger(__name__)

def create_sync_manager(config: Dict[str, Any]) -> Optional[SyncManager]:
    """
    Create sync manager instance.

    Args:
        config: Sync configuration including:
            - database: Database configuration
            - kafka: Kafka configuration
            - cache: Cache configuration
            - conflict: Conflict resolution rules
    """
    try:
        return SyncManager(
            config,
            ConflictResolver(config.get('conflict', {})),
            StateManager(config.get('state', {}))
        )
    except Exception as e:
        logger.error(f"Sync manager creation failed: {e}")
        return None

__all__ = [
    'SyncManager',
    'ConflictResolver',
    'StateManager',
    'create_sync_manager'
] 