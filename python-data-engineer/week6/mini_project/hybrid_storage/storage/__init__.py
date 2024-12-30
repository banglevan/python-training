"""
Storage Backends
--------------

Base classes and interfaces for storage implementations.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union, BinaryIO
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    def store(
        self,
        file_path: Union[str, Path],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Store file in storage backend."""
        pass
    
    @abstractmethod
    def retrieve(
        self,
        file_path: Union[str, Path],
        version: Optional[str] = None
    ) -> Dict[str, Any]:
        """Retrieve file from storage backend."""
        pass
    
    @abstractmethod
    def delete(
        self,
        file_path: Union[str, Path],
        version: Optional[str] = None
    ) -> bool:
        """Delete file from storage backend."""
        pass
    
    @abstractmethod
    def list_files(
        self,
        prefix: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List files in storage backend."""
        pass
    
    @abstractmethod
    def get_metadata(
        self,
        file_path: Union[str, Path]
    ) -> Dict[str, Any]:
        """Get file metadata."""
        pass
    
    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        pass
    
    @abstractmethod
    def close(self):
        """Close storage connection."""
        pass

# Import specific implementations
from .local import LocalStorage
from .s3 import S3Storage
from .hdfs import HDFSStorage
from .ceph import CephStorage

__all__ = [
    'StorageBackend',
    'LocalStorage',
    'S3Storage',
    'HDFSStorage',
    'CephStorage'
] 