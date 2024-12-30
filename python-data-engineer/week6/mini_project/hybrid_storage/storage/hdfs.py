"""
HDFS Storage Backend
-----------------

Hadoop Distributed File System storage implementation.
"""

from hdfs import InsecureClient, HdfsError
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, BinaryIO
import logging
import hashlib
import tempfile
from concurrent.futures import ThreadPoolExecutor
import os

from . import StorageBackend

logger = logging.getLogger(__name__)

class HDFSStorage(StorageBackend):
    """HDFS storage implementation."""
    
    def __init__(
        self,
        config: Dict[str, Any]
    ):
        """Initialize HDFS storage."""
        self.client = InsecureClient(
            f"http://{config['namenode']}:{config['port']}",
            user=config['user']
        )
        
        # Set base paths
        self.base_path = '/hybrid_storage'
        self.data_path = f"{self.base_path}/data"
        self.meta_path = f"{self.base_path}/metadata"
        
        # Initialize thread pool
        self.executor = ThreadPoolExecutor(
            max_workers=config.get('threads', 4)
        )
        
        # Ensure directories exist
        self._ensure_directories()
        
        logger.info(
            f"Initialized HDFSStorage at {self.base_path}"
        )
    
    def store(
        self,
        file_path: Union[str, Path],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Store file in HDFS."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            # Generate HDFS path
            file_hash = self._calculate_hash(file_path)
            hdfs_path = (
                f"{self.data_path}/"
                f"{file_hash[:2]}/"
                f"{file_hash[2:4]}/"
                f"{file_hash}"
            )
            
            # Prepare metadata
            meta = {
                'original_path': str(file_path),
                'hash': file_hash,
                'hdfs_path': hdfs_path,
                'size': file_path.stat().st_size,
                'timestamp': datetime.now().isoformat(),
                'metadata': metadata or {}
            }
            
            # Upload file
            with open(file_path, 'rb') as f:
                self.client.upload(
                    hdfs_path,
                    f,
                    overwrite=True
                )
            
            # Store metadata
            meta_path = f"{self.meta_path}/{file_hash}.json"
            self.client.write(
                meta_path,
                json.dumps(meta).encode(),
                overwrite=True
            )
            
            # Get HDFS file status
            status = self.client.status(hdfs_path)
            
            return {
                'path': hdfs_path,
                'hash': file_hash,
                'size': status['length'],
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
        """Retrieve file from HDFS."""
        try:
            # Get HDFS path
            if file_path.startswith(self.data_path):
                hdfs_path = file_path
                file_hash = Path(hdfs_path).name
            else:
                meta = self.get_metadata(file_path)
                hdfs_path = meta['hdfs_path']
                file_hash = meta['hash']
            
            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                # Download file
                self.client.download(
                    hdfs_path,
                    temp.name
                )
                
                # Get file status
                status = self.client.status(hdfs_path)
                
                return {
                    'path': temp.name,
                    'hash': file_hash,
                    'size': status['length'],
                    'metadata': self.get_metadata(hdfs_path)
                }
            
        except Exception as e:
            logger.error(f"Retrieve operation failed: {e}")
            raise
    
    def delete(
        self,
        file_path: Union[str, Path],
        version: Optional[str] = None
    ) -> bool:
        """Delete file from HDFS."""
        try:
            # Get HDFS path
            if file_path.startswith(self.data_path):
                hdfs_path = file_path
                file_hash = Path(hdfs_path).name
            else:
                meta = self.get_metadata(file_path)
                hdfs_path = meta['hdfs_path']
                file_hash = meta['hash']
            
            # Delete data file
            self.client.delete(hdfs_path)
            
            # Delete metadata
            meta_path = f"{self.meta_path}/{file_hash}.json"
            self.client.delete(meta_path)
            
            return True
            
        except Exception as e:
            logger.error(f"Delete operation failed: {e}")
            raise
    
    def list_files(
        self,
        prefix: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List files in HDFS storage."""
        try:
            results = []
            
            # List metadata files
            meta_files = self.client.list(self.meta_path)
            
            for meta_file in meta_files:
                try:
                    meta_path = f"{self.meta_path}/{meta_file}"
                    content = self.client.read(meta_path)
                    meta = json.loads(content.decode())
                    
                    if prefix is None or \
                       meta['original_path'].startswith(prefix):
                        # Get file status
                        status = self.client.status(
                            meta['hdfs_path']
                        )
                        
                        results.append({
                            'path': meta['hdfs_path'],
                            'original_path': meta['original_path'],
                            'hash': meta['hash'],
                            'size': status['length'],
                            'timestamp': meta['timestamp'],
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
        """Get file metadata from HDFS."""
        try:
            # Handle both HDFS paths and original paths
            if file_path.startswith(self.data_path):
                file_hash = Path(file_path).name
            else:
                file_path = Path(file_path)
                file_hash = self._calculate_hash(file_path)
            
            # Read metadata file
            meta_path = f"{self.meta_path}/{file_hash}.json"
            content = self.client.read(meta_path)
            
            return json.loads(content.decode())
            
        except Exception as e:
            logger.error(f"Metadata retrieval failed: {e}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Get HDFS storage statistics."""
        try:
            total_size = 0
            file_count = 0
            
            # Get storage metrics
            metrics = self.client.content(
                self.data_path,
                recursive=True
            )
            
            # Calculate totals
            for path, info in metrics.items():
                if info['type'] == 'FILE':
                    total_size += info['length']
                    file_count += 1
            
            # Get HDFS summary
            summary = self.client.df()
            
            return {
                'total_space': summary['total'],
                'used_space': summary['used'],
                'free_space': summary['free'],
                'data_size': total_size,
                'file_count': file_count
            }
            
        except Exception as e:
            logger.error(f"Stats retrieval failed: {e}")
            raise
    
    def close(self):
        """Clean up resources."""
        self.executor.shutdown()
    
    def _ensure_directories(self):
        """Create necessary HDFS directories."""
        try:
            # Create base directories
            for path in [self.base_path, self.data_path, self.meta_path]:
                if not self.client.status(path, strict=False):
                    self.client.makedirs(path)
            
        except Exception as e:
            logger.error(f"Directory creation failed: {e}")
            raise
    
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