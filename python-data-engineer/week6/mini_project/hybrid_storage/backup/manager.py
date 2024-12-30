"""
Backup Manager
-----------

Coordinates backup operations with:
1. Strategy selection
2. Job scheduling
3. Status tracking
4. Recovery management
"""

from typing import Dict, Any, Optional, List, Union
from pathlib import Path
import logging
import json
from datetime import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor
import tempfile
import shutil

from .strategy import BackupStrategy
from .scheduler import BackupScheduler

logger = logging.getLogger(__name__)

class BackupManager:
    """Backup system coordinator."""
    
    def __init__(
        self,
        config: Dict[str, Any],
        storage_backends: Dict[str, Any]
    ):
        """Initialize backup manager."""
        self.storage = storage_backends
        self.strategies = self._init_strategies(
            config['strategies']
        )
        
        # Initialize scheduler
        self.scheduler = BackupScheduler(
            config.get('scheduler', {})
        )
        
        # Initialize thread pool
        self.executor = ThreadPoolExecutor(
            max_workers=config.get('threads', 4)
        )
        
        # Schedule backup jobs
        self._schedule_backups()
        
        logger.info("Initialized BackupManager")
    
    async def create_backup(
        self,
        strategy_name: str,
        files: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Create backup using specified strategy."""
        try:
            strategy = self.strategies[strategy_name]
            
            # Get files to backup if not provided
            if files is None:
                files = await self._get_files_to_backup()
            
            # Filter excluded files
            files = [
                f for f in files
                if not any(
                    exc in str(f['path'])
                    for exc in strategy.exclude
                )
            ]
            
            # Get destination storage
            storage = self.storage[strategy.destination]
            
            # Create backup
            result = await asyncio.to_thread(
                strategy.create_backup,
                files,
                storage
            )
            
            # Cleanup old backups
            await asyncio.to_thread(
                strategy.cleanup_old_backups,
                storage
            )
            
            return result
            
        except Exception as e:
            logger.error(
                f"Backup creation failed: {e}"
            )
            raise
    
    async def restore_backup(
        self,
        backup_id: str,
        strategy_name: str,
        target_path: Optional[Path] = None,
        files: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Restore from backup."""
        try:
            strategy = self.strategies[strategy_name]
            storage = self.storage[strategy.destination]
            
            result = await asyncio.to_thread(
                strategy.restore_backup,
                backup_id,
                storage,
                target_path,
                files
            )
            
            return result
            
        except Exception as e:
            logger.error(
                f"Backup restoration failed: {e}"
            )
            raise
    
    def list_backups(
        self,
        strategy_name: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """List available backups."""
        try:
            results = {}
            
            strategies = (
                [self.strategies[strategy_name]]
                if strategy_name
                else self.strategies.values()
            )
            
            for strategy in strategies:
                storage = self.storage[
                    strategy.destination
                ]
                results[strategy.name] = strategy.list_backups(
                    storage
                )
            
            return results
            
        except Exception as e:
            logger.error(
                f"Backup listing failed: {e}"
            )
            raise
    
    async def verify_backup(
        self,
        backup_id: str,
        strategy_name: str
    ) -> Dict[str, Any]:
        """Verify backup integrity."""
        try:
            strategy = self.strategies[strategy_name]
            storage = self.storage[strategy.destination]
            
            result = await asyncio.to_thread(
                strategy.verify_backup,
                backup_id,
                storage
            )
            
            return result
            
        except Exception as e:
            logger.error(
                f"Backup verification failed: {e}"
            )
            raise
    
    def get_schedule(self) -> List[Dict[str, Any]]:
        """Get backup schedule."""
        return self.scheduler.list_jobs()
    
    async def run_scheduled(self):
        """Run scheduled backups."""
        try:
            jobs = self.scheduler.list_jobs()
            
            for job in jobs:
                if job['status'] == 'scheduled':
                    strategy_name = job['name']
                    try:
                        await self.create_backup(
                            strategy_name
                        )
                    except Exception as e:
                        logger.error(
                            f"Scheduled backup failed for "
                            f"{strategy_name}: {e}"
                        )
            
        except Exception as e:
            logger.error(
                f"Scheduled backup run failed: {e}"
            )
            raise
    
    def start(self):
        """Start backup scheduler."""
        self.scheduler.start()
    
    def stop(self):
        """Stop backup scheduler."""
        self.scheduler.stop()
        self.executor.shutdown()
    
    def _init_strategies(
        self,
        config: Dict[str, Any]
    ) -> Dict[str, BackupStrategy]:
        """Initialize backup strategies."""
        return {
            name: BackupStrategy(name, strategy_config)
            for name, strategy_config in config.items()
        }
    
    def _schedule_backups(self):
        """Schedule configured backups."""
        try:
            for name, strategy in self.strategies.items():
                if hasattr(strategy, 'schedule'):
                    self.scheduler.add_job(
                        name=name,
                        schedule=strategy.schedule,
                        func=self.create_backup,
                        args=[name]
                    )
            
        except Exception as e:
            logger.error(
                f"Backup scheduling failed: {e}"
            )
            raise
    
    async def _get_files_to_backup(
        self
    ) -> List[Dict[str, Any]]:
        """Get list of files to backup."""
        try:
            files = []
            
            # Get files from each storage
            for storage in self.storage.values():
                try:
                    storage_files = await asyncio.to_thread(
                        storage.list_files
                    )
                    files.extend(storage_files)
                except Exception as e:
                    logger.error(
                        f"Failed to list files from "
                        f"{storage.__class__.__name__}: {e}"
                    )
            
            return files
            
        except Exception as e:
            logger.error(
                f"File listing failed: {e}"
            )
            raise 