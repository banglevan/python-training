"""
Hybrid Storage System Core
------------------------

Main system coordinator that manages:
1. Storage backends
2. Tiering policies
3. Backup operations
4. System monitoring
"""

import logging
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
from datetime import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor

from .storage import (
    LocalStorage,
    S3Storage,
    HDFSStorage,
    CephStorage
)
from .tiering import TieringManager
from .backup import BackupManager
from .monitor import MonitoringSystem

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HybridStorage:
    """Hybrid Storage System coordinator."""
    
    def __init__(
        self,
        config_path: Union[str, Path]
    ):
        """Initialize the storage system."""
        self.config = self._load_config(config_path)
        
        # Initialize components
        self._init_storage_backends()
        self._init_tiering()
        self._init_backup()
        self._init_monitoring()
        
        # Create thread pool
        self.executor = ThreadPoolExecutor(
            max_workers=self.config.get('core', {}).get(
                'max_workers',
                4
            )
        )
        
        logger.info("Hybrid Storage System initialized")
    
    def store(
        self,
        file_path: Union[str, Path],
        tier: str = 'auto',
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Store data with automatic tiering."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            # Determine storage tier
            if tier == 'auto':
                tier = self.tiering.determine_tier(file_path)
            
            # Get storage backend
            storage = self._get_storage_for_tier(tier)
            
            # Store file
            result = storage.store(
                file_path,
                metadata=metadata
            )
            
            # Update tiering metadata
            self.tiering.update_metadata(
                file_path,
                tier,
                result
            )
            
            # Update metrics
            self.monitor.record_operation(
                'store',
                {
                    'file': str(file_path),
                    'tier': tier,
                    'size': file_path.stat().st_size
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Store operation failed: {e}")
            self.monitor.record_error('store', str(e))
            raise
    
    def retrieve(
        self,
        file_path: Union[str, Path],
        version: Optional[str] = None
    ) -> Dict[str, Any]:
        """Retrieve data from any tier."""
        try:
            # Get file location
            location = self.tiering.get_file_location(
                file_path,
                version
            )
            
            if not location:
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            # Get storage backend
            storage = self._get_storage_for_tier(
                location['tier']
            )
            
            # Retrieve file
            result = storage.retrieve(
                location['path'],
                version=version
            )
            
            # Update access metadata
            self.tiering.update_access(
                file_path,
                location['tier']
            )
            
            # Update metrics
            self.monitor.record_operation(
                'retrieve',
                {
                    'file': str(file_path),
                    'tier': location['tier'],
                    'version': version
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Retrieve operation failed: {e}")
            self.monitor.record_error('retrieve', str(e))
            raise
    
    def delete(
        self,
        file_path: Union[str, Path],
        version: Optional[str] = None
    ) -> bool:
        """Delete data from all tiers."""
        try:
            # Get file locations
            locations = self.tiering.get_all_locations(
                file_path,
                version
            )
            
            if not locations:
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            # Delete from all locations
            for location in locations:
                storage = self._get_storage_for_tier(
                    location['tier']
                )
                storage.delete(
                    location['path'],
                    version=version
                )
            
            # Update metadata
            self.tiering.remove_metadata(
                file_path,
                version
            )
            
            # Update metrics
            self.monitor.record_operation(
                'delete',
                {
                    'file': str(file_path),
                    'version': version
                }
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Delete operation failed: {e}")
            self.monitor.record_error('delete', str(e))
            raise
    
    def _load_config(
        self,
        config_path: Union[str, Path]
    ) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _init_storage_backends(self):
        """Initialize storage backends."""
        self.storage = {
            'local': LocalStorage(
                self.config['storage']['local']
            ),
            's3': S3Storage(
                self.config['storage']['s3']
            ),
            'hdfs': HDFSStorage(
                self.config['storage']['hdfs']
            ),
            'ceph': CephStorage(
                self.config['storage']['ceph']
            )
        }
    
    def _init_tiering(self):
        """Initialize tiering manager."""
        self.tiering = TieringManager(
            self.config['tiering'],
            self.storage
        )
    
    def _init_backup(self):
        """Initialize backup manager."""
        self.backup = BackupManager(
            self.config['backup'],
            self.storage
        )
    
    def _init_monitoring(self):
        """Initialize monitoring system."""
        self.monitor = MonitoringSystem(
            self.config['monitoring']
        )
    
    def _get_storage_for_tier(
        self,
        tier: str
    ):
        """Get storage backend for tier."""
        tier_config = self.config['tiering']['policies'][tier]
        return self.storage[tier_config['storage']]
    
    async def run_maintenance(self):
        """Run system maintenance tasks."""
        try:
            # Check tiering policies
            await self.tiering.check_policies()
            
            # Run backup tasks
            await self.backup.run_scheduled()
            
            # Update monitoring
            await self.monitor.update_metrics()
            
            # Check alerts
            alerts = await self.monitor.check_alerts()
            if alerts:
                logger.warning(f"Active alerts: {alerts}")
            
        except Exception as e:
            logger.error(f"Maintenance failed: {e}")
            self.monitor.record_error(
                'maintenance',
                str(e)
            )
    
    def close(self):
        """Clean up resources."""
        try:
            # Close storage connections
            for storage in self.storage.values():
                storage.close()
            
            # Stop backup scheduler
            self.backup.stop()
            
            # Stop monitoring
            self.monitor.stop()
            
            # Close thread pool
            self.executor.shutdown()
            
            logger.info("Hybrid Storage System shut down")
            
        except Exception as e:
            logger.error(f"Shutdown error: {e}")
            raise

def main():
    """Example usage."""
    storage = HybridStorage('config.yaml')
    
    try:
        # Store file
        result = storage.store(
            'data.txt',
            tier='auto',
            metadata={'purpose': 'testing'}
        )
        print(f"Store result: {result}")
        
        # Retrieve file
        data = storage.retrieve('data.txt')
        print(f"Retrieved data: {data}")
        
        # Run maintenance
        asyncio.run(storage.run_maintenance())
        
    finally:
        storage.close()

if __name__ == '__main__':
    main() 