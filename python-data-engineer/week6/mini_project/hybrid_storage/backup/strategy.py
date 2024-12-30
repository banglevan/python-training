"""
Backup Strategies
--------------

Implements different backup approaches:
1. Full backup
2. Incremental backup
3. Differential backup
4. Snapshot backup
"""

from typing import Dict, Any, Optional, List, Union
from pathlib import Path
import logging
import json
from datetime import datetime
import hashlib
import tempfile
import shutil
import os
from typing import BinaryIO
logger = logging.getLogger(__name__)

class BackupStrategy:
    """Base class for backup strategies."""
    
    def __init__(
        self,
        name: str,
        config: Dict[str, Any]
    ):
        """Initialize backup strategy."""
        self.name = name
        self.retention = config.get('retention', 7)
        self.destination = config['destination']
        self.compress = config.get('compress', True)
        self.encrypt = config.get('encrypt', False)
        self.exclude = config.get('exclude', [])
        
        logger.info(
            f"Initialized backup strategy: {self.name}"
        )
    
    def prepare_backup(
        self,
        files: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Prepare backup metadata."""
        try:
            backup_id = self._generate_backup_id()
            
            metadata = {
                'backup_id': backup_id,
                'strategy': self.name,
                'timestamp': datetime.now().isoformat(),
                'file_count': len(files),
                'total_size': sum(f['size'] for f in files),
                'files': files
            }
            
            return metadata
            
        except Exception as e:
            logger.error(f"Backup preparation failed: {e}")
            raise
    
    def create_backup(
        self,
        files: List[Dict[str, Any]],
        storage: Any
    ) -> Dict[str, Any]:
        """Create backup using strategy."""
        raise NotImplementedError
    
    def restore_backup(
        self,
        backup_id: str,
        storage: Any,
        target_path: Optional[Path] = None
    ) -> Dict[str, Any]:
        """Restore from backup."""
        raise NotImplementedError
    
    def cleanup_old_backups(
        self,
        storage: Any
    ) -> List[str]:
        """Remove old backups based on retention policy."""
        try:
            # List backups
            backups = self.list_backups(storage)
            
            # Sort by timestamp
            backups.sort(
                key=lambda x: x['timestamp'],
                reverse=True
            )
            
            # Keep only recent backups
            to_delete = backups[self.retention:]
            
            # Delete old backups
            deleted = []
            for backup in to_delete:
                try:
                    self._delete_backup(
                        backup['backup_id'],
                        storage
                    )
                    deleted.append(backup['backup_id'])
                except Exception as e:
                    logger.error(
                        f"Failed to delete backup "
                        f"{backup['backup_id']}: {e}"
                    )
            
            return deleted
            
        except Exception as e:
            logger.error(f"Backup cleanup failed: {e}")
            raise
    
    def list_backups(
        self,
        storage: Any
    ) -> List[Dict[str, Any]]:
        """List available backups."""
        try:
            backups = []
            
            # List backup metadata files
            files = storage.list_files(
                prefix=f"backups/{self.name}/"
            )
            
            for file in files:
                if file['path'].endswith('metadata.json'):
                    try:
                        # Read metadata
                        content = storage.retrieve(
                            file['path']
                        )
                        metadata = json.loads(
                            content['data'].read().decode()
                        )
                        backups.append(metadata)
                    except Exception as e:
                        logger.warning(
                            f"Failed to read backup metadata "
                            f"{file['path']}: {e}"
                        )
            
            return backups
            
        except Exception as e:
            logger.error(f"Backup listing failed: {e}")
            raise
    
    def verify_backup(
        self,
        backup_id: str,
        storage: Any
    ) -> Dict[str, Any]:
        """Verify backup integrity."""
        try:
            # Get backup metadata
            meta_path = (
                f"backups/{self.name}/"
                f"{backup_id}/metadata.json"
            )
            content = storage.retrieve(meta_path)
            metadata = json.loads(
                content['data'].read().decode()
            )
            
            # Verify each file
            results = {
                'backup_id': backup_id,
                'verified': 0,
                'failed': 0,
                'missing': 0,
                'details': []
            }
            
            for file_info in metadata['files']:
                try:
                    # Check file exists
                    path = (
                        f"backups/{self.name}/"
                        f"{backup_id}/files/"
                        f"{file_info['hash']}"
                    )
                    if not storage.exists(path):
                        results['missing'] += 1
                        results['details'].append({
                            'path': file_info['path'],
                            'status': 'missing'
                        })
                        continue
                    
                    # Verify hash
                    content = storage.retrieve(path)
                    current_hash = self._calculate_hash(
                        content['data']
                    )
                    
                    if current_hash == file_info['hash']:
                        results['verified'] += 1
                        results['details'].append({
                            'path': file_info['path'],
                            'status': 'verified'
                        })
                    else:
                        results['failed'] += 1
                        results['details'].append({
                            'path': file_info['path'],
                            'status': 'failed',
                            'error': 'Hash mismatch'
                        })
                    
                except Exception as e:
                    results['failed'] += 1
                    results['details'].append({
                        'path': file_info['path'],
                        'status': 'failed',
                        'error': str(e)
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"Backup verification failed: {e}")
            raise
    
    def _generate_backup_id(self) -> str:
        """Generate unique backup ID."""
        timestamp = datetime.now().strftime(
            '%Y%m%d_%H%M%S'
        )
        random = os.urandom(4).hex()
        return f"{self.name}_{timestamp}_{random}"
    
    def _calculate_hash(
        self,
        data: Union[bytes, BinaryIO],
        chunk_size: int = 8192
    ) -> str:
        """Calculate SHA-256 hash of data."""
        hasher = hashlib.sha256()
        
        if isinstance(data, bytes):
            hasher.update(data)
        else:
            while chunk := data.read(chunk_size):
                hasher.update(chunk)
        
        return hasher.hexdigest()
    
    def _delete_backup(
        self,
        backup_id: str,
        storage: Any
    ):
        """Delete backup files and metadata."""
        try:
            # Delete backup files
            prefix = f"backups/{self.name}/{backup_id}/"
            files = storage.list_files(prefix=prefix)
            
            for file in files:
                storage.delete(file['path'])
            
        except Exception as e:
            logger.error(
                f"Failed to delete backup {backup_id}: {e}"
            )
            raise 