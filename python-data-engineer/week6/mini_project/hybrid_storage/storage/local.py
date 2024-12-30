"""
Local Storage Backend
------------------

File system based storage implementation.
"""

import shutil
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, BinaryIO
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor

from . import StorageBackend

logger = logging.getLogger(__name__)

class LocalStorage(StorageBackend):
    """Local file system storage implementation."""
    
    def __init__(
        self,
        config: Dict[str, Any]
    ):
        """Initialize local storage."""
        self.base_path = Path(config['base_path'])
        self.max_size = self._parse_size(config['max_size'])
        self.reserved_space = self._parse_size(
            config['reserved_space']
        )
        
        # Create directories
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.data_path = self.base_path / 'data'
        self.data_path.mkdir(exist_ok=True)
        self.meta_path = self.base_path / 'metadata'
        self.meta_path.mkdir(exist_ok=True)
        
        # Initialize thread pool
        self.executor = ThreadPoolExecutor(
            max_workers=config.get('threads', 4)
        )
        
        logger.info(
            f"Initialized LocalStorage at {self.base_path}"
        )
    
    def store(
        self,
        file_path: Union[str, Path],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Store file in local storage."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            # Check space
            if not self._check_space(file_path):
                raise OSError("Insufficient storage space")
            
            # Generate storage path
            file_hash = self._calculate_hash(file_path)
            rel_path = Path(file_hash[:2]) / file_hash[2:4] / file_hash
            store_path = self.data_path / rel_path
            
            # Create parent directories
            store_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy file
            shutil.copy2(file_path, store_path)
            
            # Store metadata
            meta = {
                'original_path': str(file_path),
                'hash': file_hash,
                'size': file_path.stat().st_size,
                'created': datetime.now().isoformat(),
                'metadata': metadata or {}
            }
            
            meta_path = self.meta_path / f"{file_hash}.json"
            with open(meta_path, 'w') as f:
                json.dump(meta, f, indent=2)
            
            return {
                'path': str(rel_path),
                'hash': file_hash,
                'size': meta['size'],
                'metadata': meta
            }
            
        except Exception as e:
            logger.error(f"Store operation failed: {e}")
            raise
    
    def retrieve(
        self,
        file_path: Union[str, Path],
        version: Optional[str] = None
    ) -> Dict[str, Any]:
        """Retrieve file from local storage."""
        try:
            # Handle both full paths and hashes
            if isinstance(file_path, str) and len(file_path) == 64:
                file_hash = file_path
            else:
                file_path = Path(file_path)
                meta = self.get_metadata(file_path)
                file_hash = meta['hash']
            
            # Construct storage path
            store_path = self.data_path / \
                file_hash[:2] / file_hash[2:4] / file_hash
            
            if not store_path.exists():
                raise FileNotFoundError(
                    f"File not found in storage: {file_path}"
                )
            
            # Get metadata
            meta = self.get_metadata(file_hash)
            
            return {
                'path': str(store_path),
                'hash': file_hash,
                'size': store_path.stat().st_size,
                'metadata': meta
            }
            
        except Exception as e:
            logger.error(f"Retrieve operation failed: {e}")
            raise
    
    def delete(
        self,
        file_path: Union[str, Path],
        version: Optional[str] = None
    ) -> bool:
        """Delete file from local storage."""
        try:
            # Get file hash
            if isinstance(file_path, str) and len(file_path) == 64:
                file_hash = file_path
            else:
                file_path = Path(file_path)
                meta = self.get_metadata(file_path)
                file_hash = meta['hash']
            
            # Delete data file
            store_path = self.data_path / \
                file_hash[:2] / file_hash[2:4] / file_hash
            if store_path.exists():
                store_path.unlink()
            
            # Delete metadata
            meta_path = self.meta_path / f"{file_hash}.json"
            if meta_path.exists():
                meta_path.unlink()
            
            return True
            
        except Exception as e:
            logger.error(f"Delete operation failed: {e}")
            raise
    
    def list_files(
        self,
        prefix: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List files in local storage."""
        try:
            results = []
            
            # Walk through metadata directory
            for meta_file in self.meta_path.glob("*.json"):
                try:
                    with open(meta_file, 'r') as f:
                        meta = json.load(f)
                    
                    if prefix is None or \
                       meta['original_path'].startswith(prefix):
                        results.append({
                            'path': meta['original_path'],
                            'hash': meta['hash'],
                            'size': meta['size'],
                            'created': meta['created'],
                            'metadata': meta.get('metadata', {})
                        })
                except Exception as e:
                    logger.warning(
                        f"Error reading metadata {meta_file}: {e}"
                    )
            
            return results
            
        except Exception as e:
            logger.error(f"List operation failed: {e}")
            raise
    
    def get_metadata(
        self,
        file_path: Union[str, Path]
    ) -> Dict[str, Any]:
        """Get file metadata."""
        try:
            # Handle both paths and hashes
            if isinstance(file_path, str) and len(file_path) == 64:
                file_hash = file_path
            else:
                file_path = Path(file_path)
                file_hash = self._calculate_hash(file_path)
            
            meta_path = self.meta_path / f"{file_hash}.json"
            if not meta_path.exists():
                raise FileNotFoundError(
                    f"Metadata not found for: {file_path}"
                )
            
            with open(meta_path, 'r') as f:
                return json.load(f)
            
        except Exception as e:
            logger.error(f"Metadata retrieval failed: {e}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        try:
            total, used, free = shutil.disk_usage(self.base_path)
            
            return {
                'total_space': total,
                'used_space': used,
                'free_space': free,
                'used_percent': (used / total) * 100,
                'file_count': len(list(self.meta_path.glob("*.json"))),
                'data_size': self._get_directory_size(self.data_path)
            }
            
        except Exception as e:
            logger.error(f"Stats retrieval failed: {e}")
            raise
    
    def close(self):
        """Clean up resources."""
        self.executor.shutdown()
    
    def _calculate_hash(
        self,
        file_path: Path,
        chunk_size: int = 8192
    ) -> str:
        """Calculate SHA-256 hash of file."""
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    def _check_space(
        self,
        file_path: Path
    ) -> bool:
        """Check if enough space is available."""
        total, used, free = shutil.disk_usage(self.base_path)
        required = file_path.stat().st_size
        
        return (free - self.reserved_space) >= required
    
    def _get_directory_size(
        self,
        path: Path
    ) -> int:
        """Calculate total size of directory."""
        return sum(
            f.stat().st_size
            for f in path.rglob('*')
            if f.is_file()
        )
    
    def _parse_size(
        self,
        size_str: str
    ) -> int:
        """Parse size string (e.g., '1TB') to bytes."""
        units = {
            'B': 1,
            'KB': 1024,
            'MB': 1024**2,
            'GB': 1024**3,
            'TB': 1024**4
        }
        
        size = size_str.strip()
        for unit, multiplier in units.items():
            if size.endswith(unit):
                return int(float(size[:-len(unit)]) * multiplier)
        
        return int(size) 