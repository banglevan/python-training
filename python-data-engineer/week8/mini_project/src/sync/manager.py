"""
Sync manager implementation.
"""

import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2.extras import RealDictCursor

from ..connectors import DataConnector
from .conflict import ConflictResolver
from .state import StateManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SyncManager:
    """Manages data synchronization across sources."""
    
    def __init__(
        self,
        config: Dict[str, Any],
        conflict_resolver: ConflictResolver,
        state_manager: StateManager
    ):
        """Initialize sync manager."""
        self.config = config
        self.conflict_resolver = conflict_resolver
        self.state_manager = state_manager
        self.db_config = config['database']
        self.max_workers = config.get('max_workers', 5)
        self.batch_size = config.get('batch_size', 100)
        self._connectors: Dict[str, DataConnector] = {}
        
        # Initialize thread pool
        self.executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="sync_worker"
        )
    
    def register_connector(
        self,
        name: str,
        connector: DataConnector
    ) -> bool:
        """Register data source connector."""
        try:
            if name in self._connectors:
                logger.warning(f"Connector {name} already registered")
                return False
            
            self._connectors[name] = connector
            logger.info(f"Registered connector: {name}")
            return True
            
        except Exception as e:
            logger.error(f"Connector registration failed: {e}")
            return False
    
    def start_sync(
        self,
        source_name: str,
        target_names: List[str],
        entity_type: str
    ) -> bool:
        """Start synchronization process."""
        try:
            source = self._connectors.get(source_name)
            if not source:
                logger.error(f"Source not found: {source_name}")
                return False
            
            targets = []
            for name in target_names:
                target = self._connectors.get(name)
                if target:
                    targets.append(target)
                else:
                    logger.error(f"Target not found: {name}")
            
            if not targets:
                logger.error("No valid targets found")
                return False
            
            # Start sync process
            sync_id = str(uuid.uuid4())
            logger.info(f"Starting sync {sync_id} for {entity_type}")
            
            self._record_sync_start(sync_id, source_name, target_names, entity_type)
            
            # Submit sync job
            self.executor.submit(
                self._sync_process,
                sync_id,
                source,
                targets,
                entity_type
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Sync start failed: {e}")
            return False
    
    def _sync_process(
        self,
        sync_id: str,
        source: DataConnector,
        targets: List[DataConnector],
        entity_type: str
    ):
        """Execute sync process."""
        try:
            processed_count = 0
            error_count = 0
            
            for event in source.get_events():
                try:
                    # Check entity type
                    if not event.event_type.startswith(entity_type):
                        continue
                    
                    # Get current state
                    current_state = self.state_manager.get_state(
                        event.data.get('id'),
                        event.source_type
                    )
                    
                    # Check for conflicts
                    if current_state and self._has_conflict(current_state, event):
                        resolution = self.conflict_resolver.resolve(
                            current_state,
                            event
                        )
                        if not resolution:
                            logger.warning(f"Conflict unresolved for {event.event_id}")
                            continue
                    
                    # Update state
                    new_state = self.state_manager.update_state(
                        event.data.get('id'),
                        event.source_type,
                        event.data
                    )
                    
                    # Sync to targets
                    for target in targets:
                        try:
                            self._sync_to_target(target, new_state)
                        except Exception as e:
                            logger.error(f"Target sync failed: {e}")
                            error_count += 1
                    
                    processed_count += 1
                    
                    # Acknowledge event
                    source.acknowledge_event(event.event_id)
                    
                except Exception as e:
                    logger.error(f"Event processing failed: {e}")
                    error_count += 1
                
                # Check batch size
                if processed_count >= self.batch_size:
                    self._record_sync_progress(
                        sync_id,
                        processed_count,
                        error_count
                    )
                    processed_count = 0
                    error_count = 0
            
            self._record_sync_completion(sync_id)
            
        except Exception as e:
            logger.error(f"Sync process failed: {e}")
            self._record_sync_error(sync_id, str(e))
    
    def _has_conflict(
        self,
        current_state: Dict[str, Any],
        event: Any
    ) -> bool:
        """Check for state conflicts."""
        return self.conflict_resolver.check_conflict(current_state, event)
    
    def _sync_to_target(
        self,
        target: DataConnector,
        state: Dict[str, Any]
    ):
        """Sync state to target."""
        # Implementation depends on target connector
        pass
    
    def _record_sync_start(
        self,
        sync_id: str,
        source: str,
        targets: List[str],
        entity_type: str
    ):
        """Record sync start in database."""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO sync_jobs
                        (sync_id, source, targets, entity_type,
                         status, started_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        sync_id,
                        source,
                        json.dumps(targets),
                        entity_type,
                        'running',
                        datetime.now()
                    ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to record sync start: {e}")
    
    def _record_sync_progress(
        self,
        sync_id: str,
        processed: int,
        errors: int
    ):
        """Record sync progress."""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE sync_jobs
                        SET processed_count = processed_count + %s,
                            error_count = error_count + %s,
                            updated_at = %s
                        WHERE sync_id = %s
                    """, (
                        processed,
                        errors,
                        datetime.now(),
                        sync_id
                    ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to record sync progress: {e}")
    
    def _record_sync_completion(self, sync_id: str):
        """Record sync completion."""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE sync_jobs
                        SET status = 'completed',
                            completed_at = %s
                        WHERE sync_id = %s
                    """, (
                        datetime.now(),
                        sync_id
                    ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to record sync completion: {e}")
    
    def _record_sync_error(self, sync_id: str, error: str):
        """Record sync error."""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE sync_jobs
                        SET status = 'failed',
                            error = %s,
                            completed_at = %s
                        WHERE sync_id = %s
                    """, (
                        error,
                        datetime.now(),
                        sync_id
                    ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to record sync error: {e}")
    
    def close(self):
        """Close sync manager."""
        try:
            self.executor.shutdown(wait=True)
            for connector in self._connectors.values():
                connector.close()
        except Exception as e:
            logger.error(f"Sync manager cleanup failed: {e}") 