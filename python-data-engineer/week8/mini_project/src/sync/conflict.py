"""
Conflict resolution implementation.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
from enum import Enum
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConflictType(Enum):
    """Types of data conflicts."""
    VERSION_MISMATCH = 'version_mismatch'
    CONCURRENT_UPDATE = 'concurrent_update'
    DATA_DIVERGENCE = 'data_divergence'
    DELETION_CONFLICT = 'deletion_conflict'

class ResolutionStrategy(Enum):
    """Conflict resolution strategies."""
    LATEST_WINS = 'latest_wins'
    SOURCE_WINS = 'source_wins'
    MANUAL_REVIEW = 'manual_review'
    MERGE = 'merge'

class ConflictResolver:
    """Handles data synchronization conflicts."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize conflict resolver."""
        self.config = config
        self.default_strategy = ResolutionStrategy(
            config.get('default_strategy', 'latest_wins')
        )
        self.source_priorities = config.get('source_priorities', {})
        self.merge_rules = config.get('merge_rules', {})
    
    def check_conflict(
        self,
        current_state: Dict[str, Any],
        event: Any
    ) -> bool:
        """
        Check for conflicts between current state and new event.
        
        Args:
            current_state: Current entity state
            event: New event data
        """
        try:
            # Check version conflict
            if self._check_version_conflict(current_state, event):
                return True
            
            # Check concurrent updates
            if self._check_concurrent_update(current_state, event):
                return True
            
            # Check data divergence
            if self._check_data_divergence(current_state, event):
                return True
            
            # Check deletion conflict
            if self._check_deletion_conflict(current_state, event):
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Conflict check failed: {e}")
            return True
    
    def resolve(
        self,
        current_state: Dict[str, Any],
        event: Any
    ) -> Optional[Dict[str, Any]]:
        """
        Resolve conflict between states.
        
        Args:
            current_state: Current entity state
            event: New event data
        
        Returns:
            Resolved state or None if unresolvable
        """
        try:
            conflict_type = self._determine_conflict_type(current_state, event)
            strategy = self._get_resolution_strategy(
                conflict_type,
                event.source_type
            )
            
            if strategy == ResolutionStrategy.LATEST_WINS:
                return self._resolve_by_timestamp(current_state, event)
                
            elif strategy == ResolutionStrategy.SOURCE_WINS:
                return self._resolve_by_priority(current_state, event)
                
            elif strategy == ResolutionStrategy.MERGE:
                return self._resolve_by_merge(current_state, event)
                
            elif strategy == ResolutionStrategy.MANUAL_REVIEW:
                self._queue_for_review(current_state, event)
                return None
            
            return None
            
        except Exception as e:
            logger.error(f"Conflict resolution failed: {e}")
            return None
    
    def _check_version_conflict(
        self,
        current_state: Dict[str, Any],
        event: Any
    ) -> bool:
        """Check for version conflicts."""
        try:
            current_version = current_state.get('version', 0)
            event_version = event.data.get('version', 0)
            
            return current_version > event_version
            
        except Exception as e:
            logger.error(f"Version conflict check failed: {e}")
            return True
    
    def _check_concurrent_update(
        self,
        current_state: Dict[str, Any],
        event: Any
    ) -> bool:
        """Check for concurrent updates."""
        try:
            current_timestamp = datetime.fromisoformat(
                current_state.get('updated_at', '1970-01-01T00:00:00')
            )
            event_timestamp = event.timestamp
            
            # Check if updates are within conflict window
            conflict_window = self.config.get('conflict_window_seconds', 60)
            time_diff = abs((current_timestamp - event_timestamp).total_seconds())
            
            return time_diff <= conflict_window
            
        except Exception as e:
            logger.error(f"Concurrent update check failed: {e}")
            return True
    
    def _check_data_divergence(
        self,
        current_state: Dict[str, Any],
        event: Any
    ) -> bool:
        """Check for data divergence."""
        try:
            # Compare checksums of key fields
            current_checksum = self._generate_checksum(
                current_state.get('data', {})
            )
            event_checksum = self._generate_checksum(event.data)
            
            return current_checksum != event_checksum
            
        except Exception as e:
            logger.error(f"Data divergence check failed: {e}")
            return True
    
    def _check_deletion_conflict(
        self,
        current_state: Dict[str, Any],
        event: Any
    ) -> bool:
        """Check for deletion conflicts."""
        try:
            current_deleted = current_state.get('deleted', False)
            event_deleted = event.data.get('deleted', False)
            
            return current_deleted != event_deleted
            
        except Exception as e:
            logger.error(f"Deletion conflict check failed: {e}")
            return True
    
    def _determine_conflict_type(
        self,
        current_state: Dict[str, Any],
        event: Any
    ) -> ConflictType:
        """Determine type of conflict."""
        if self._check_version_conflict(current_state, event):
            return ConflictType.VERSION_MISMATCH
        elif self._check_concurrent_update(current_state, event):
            return ConflictType.CONCURRENT_UPDATE
        elif self._check_data_divergence(current_state, event):
            return ConflictType.DATA_DIVERGENCE
        elif self._check_deletion_conflict(current_state, event):
            return ConflictType.DELETION_CONFLICT
        return ConflictType.DATA_DIVERGENCE
    
    def _get_resolution_strategy(
        self,
        conflict_type: ConflictType,
        source_type: str
    ) -> ResolutionStrategy:
        """Get resolution strategy for conflict."""
        # Check source-specific strategy
        source_strategy = self.config.get('source_strategies', {}).get(source_type)
        if source_strategy:
            return ResolutionStrategy(source_strategy)
        
        # Check conflict type strategy
        type_strategy = self.config.get('type_strategies', {}).get(conflict_type.value)
        if type_strategy:
            return ResolutionStrategy(type_strategy)
        
        return self.default_strategy
    
    def _generate_checksum(self, data: Dict[str, Any]) -> str:
        """Generate checksum for data."""
        return hashlib.sha256(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()
    
    def _resolve_by_timestamp(
        self,
        current_state: Dict[str, Any],
        event: Any
    ) -> Dict[str, Any]:
        """Resolve conflict by timestamp."""
        current_timestamp = datetime.fromisoformat(
            current_state.get('updated_at', '1970-01-01T00:00:00')
        )
        
        if event.timestamp > current_timestamp:
            return event.data
        return current_state
    
    def _resolve_by_priority(
        self,
        current_state: Dict[str, Any],
        event: Any
    ) -> Dict[str, Any]:
        """Resolve conflict by source priority."""
        current_priority = self.source_priorities.get(
            current_state.get('source_type'), 0
        )
        event_priority = self.source_priorities.get(
            event.source_type, 0
        )
        
        if event_priority >= current_priority:
            return event.data
        return current_state
    
    def _resolve_by_merge(
        self,
        current_state: Dict[str, Any],
        event: Any
    ) -> Dict[str, Any]:
        """Resolve conflict by merging states."""
        try:
            merged_state = current_state.copy()
            
            # Apply merge rules
            entity_type = event.data.get('type', 'default')
            rules = self.merge_rules.get(entity_type, {})
            
            for field, rule in rules.items():
                if field in event.data:
                    if rule == 'take_new':
                        merged_state[field] = event.data[field]
                    elif rule == 'take_existing':
                        continue
                    elif rule == 'take_latest':
                        if event.timestamp > datetime.fromisoformat(
                            current_state.get('updated_at', '1970-01-01T00:00:00')
                        ):
                            merged_state[field] = event.data[field]
            
            return merged_state
            
        except Exception as e:
            logger.error(f"Merge resolution failed: {e}")
            return current_state
    
    def _queue_for_review(
        self,
        current_state: Dict[str, Any],
        event: Any
    ):
        """Queue conflict for manual review."""
        try:
            # Implementation for manual review queueing
            logger.info(
                f"Queued for review: {event.data.get('id')} "
                f"from {event.source_type}"
            )
        except Exception as e:
            logger.error(f"Review queueing failed: {e}") 