"""
Ceph Storage Backend
-----------------

Ceph storage implementation supporting:
1. RADOS object storage
2. RBD block storage
3. CephFS file system
"""

import rados
import rbd
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

class CephStorage(StorageBackend):
    """Ceph storage implementation."""
    
    def __init__(
        self,
        config: Dict[str, Any]
    ):
        """Initialize Ceph storage."""
        # Initialize RADOS connection
        self.cluster = rados.Rados(
            conffile='/etc/ceph/ceph.conf',
            conf=dict(
                keyring=f"/etc/ceph/ceph.client.{config['user']}.keyring"
            ),
            name=f"client.{config['user']}"
        )
        
        # Connect to cluster
        self.cluster.connect()
        
        # Open pool
        self.pool = config['pool']
        self.ioctx = self.cluster.open_ioctx(self.pool)
        
        # Initialize RBD
        self.rbd = rbd.RBD()
        
        # Set namespace for hybrid storage
        self.namespace = 'hybrid_storage'
        self.ioctx.set_namespace(self.namespace)
        
        # Initialize thread pool
        self.executor = ThreadPoolExecutor(
            max_workers=config.get('threads', 4)
        )
        
        logger.info(
            f"Initialized CephStorage with pool: {self.pool}"
        )
    
    def store(
        self,
        file_path: Union[str, Path],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Store file in Ceph."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            # Generate object name
            file_hash = self._calculate_hash(file_path)
            obj_name = (
                f"{file_hash[:2]}/"
                f"{file_hash[2:4]}/"
                f"{file_hash}"
            )
            
            # Prepare metadata
            meta = {
                'original_path': str(file_path),
                'hash': file_hash,
                'size': file_path.stat().st_size,
                'timestamp': datetime.now().isoformat(),
                'metadata': metadata or {}
            }
            
            # Store file content
            with open(file_path, 'rb') as f:
                self.ioctx.write_full(obj_name, f.read())
            
            # Store metadata as xattr
            self.ioctx.set_xattr(
                obj_name,
                'metadata',
                json.dumps(meta).encode()
            )
            
            # Get object stats
            stats = self.ioctx.stat(obj_name)
            
            return {
                'path': obj_name,
                'hash': file_hash,
                'size': stats[0],
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
        """Retrieve file from Ceph."""
        try:
            # Get object name
            if '/' in str(file_path):
                obj_name = str(file_path)
                file_hash = obj_name.split('/')[-1]
            else:
                file_path = Path(file_path)
                file_hash = self._calculate_hash(file_path)
                obj_name = (
                    f"{file_hash[:2]}/"
                    f"{file_hash[2:4]}/"
                    f"{file_hash}"
                )
            
            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                # Read object
                content = self.ioctx.read(obj_name)
                temp.write(content)
                
                # Get metadata
                meta = self._get_object_metadata(obj_name)
                
                # Get stats
                stats = self.ioctx.stat(obj_name)
                
                return {
                    'path': temp.name,
                    'hash': file_hash,
                    'size': stats[0],
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
        """Delete file from Ceph."""
        try:
            # Get object name
            if '/' in str(file_path):
                obj_name = str(file_path)
            else:
                file_path = Path(file_path)
                file_hash = self._calculate_hash(file_path)
                obj_name = (
                    f"{file_hash[:2]}/"
                    f"{file_hash[2:4]}/"
                    f"{file_hash}"
                )
            
            # Remove object
            self.ioctx.remove_object(obj_name)
            return True
            
        except Exception as e:
            logger.error(f"Delete operation failed: {e}")
            raise
    
    def list_files(
        self,
        prefix: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List files in Ceph storage."""
        try:
            results = []
            
            def list_callback(obj_name):
                try:
                    # Get object stats and metadata
                    stats = self.ioctx.stat(obj_name)
                    meta = self._get_object_metadata(obj_name)
                    
                    if prefix is None or \
                       meta['original_path'].startswith(prefix):
                        results.append({
                            'path': obj_name,
                            'original_path': meta['original_path'],
                            'hash': meta['hash'],
                            'size': stats[0],
                            'timestamp': meta['timestamp'],
                            'metadata': meta.get('metadata', {})
                        })
                except Exception as e:
                    logger.warning(
                        f"Error reading object {obj_name}: {e}"
                    )
            
            # List all objects
            self.ioctx.list_objects(list_callback)
            
            return results
            
        except Exception as e:
            logger.error(f"List operation failed: {e}")
            raise
    
    def get_metadata(
        self,
        file_path: Union[str, Path]
    ) -> Dict[str, Any]:
        """Get file metadata from Ceph."""
        try:
            # Get object name
            if '/' in str(file_path):
                obj_name = str(file_path)
            else:
                file_path = Path(file_path)
                file_hash = self._calculate_hash(file_path)
                obj_name = (
                    f"{file_hash[:2]}/"
                    f"{file_hash[2:4]}/"
                    f"{file_hash}"
                )
            
            return self._get_object_metadata(obj_name)
            
        except Exception as e:
            logger.error(f"Metadata retrieval failed: {e}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Get Ceph storage statistics."""
        try:
            # Get cluster stats
            cluster_stats = self.cluster.get_cluster_stats()
            
            # Get pool stats
            pool_stats = self.ioctx.get_stats()
            
            # Count objects in namespace
            object_count = sum(
                1 for _ in self.ioctx.list_objects()
            )
            
            return {
                'total_space': cluster_stats['kb'] * 1024,
                'used_space': cluster_stats['kb_used'] * 1024,
                'available_space': cluster_stats['kb_avail'] * 1024,
                'pool_objects': pool_stats['num_objects'],
                'namespace_objects': object_count,
                'pool_usage': pool_stats['num_bytes']
            }
            
        except Exception as e:
            logger.error(f"Stats retrieval failed: {e}")
            raise
    
    def close(self):
        """Clean up resources."""
        try:
            self.executor.shutdown()
            self.ioctx.close()
            self.cluster.shutdown()
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
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
    
    def _get_object_metadata(
        self,
        obj_name: str
    ) -> Dict[str, Any]:
        """Get object metadata from xattr."""
        try:
            meta_bytes = self.ioctx.get_xattr(
                obj_name,
                'metadata'
            )
            return json.loads(meta_bytes.decode())
        except Exception as e:
            logger.error(
                f"Error reading metadata for {obj_name}: {e}"
            )
            raise 