"""
S3 Storage Backend
---------------

S3-compatible storage implementation.
"""

import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, BinaryIO
import logging
import hashlib
import mimetypes
from concurrent.futures import ThreadPoolExecutor

from . import StorageBackend

logger = logging.getLogger(__name__)

class S3Storage(StorageBackend):
    """S3-compatible storage implementation."""
    
    def __init__(
        self,
        config: Dict[str, Any]
    ):
        """Initialize S3 storage."""
        self.bucket = config['bucket']
        self.client = boto3.client(
            's3',
            endpoint_url=config.get('endpoint'),
            aws_access_key_id=config['access_key'],
            aws_secret_access_key=config['secret_key'],
            region_name=config.get('region', 'us-east-1')
        )
        
        # Initialize thread pool
        self.executor = ThreadPoolExecutor(
            max_workers=config.get('threads', 4)
        )
        
        # Ensure bucket exists
        self._ensure_bucket()
        
        logger.info(
            f"Initialized S3Storage with bucket: {self.bucket}"
        )
    
    def store(
        self,
        file_path: Union[str, Path],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Store file in S3."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            # Generate key
            file_hash = self._calculate_hash(file_path)
            key = f"{file_hash[:2]}/{file_hash[2:4]}/{file_hash}"
            
            # Prepare metadata
            s3_metadata = {
                'original_path': str(file_path),
                'hash': file_hash,
                'timestamp': datetime.now().isoformat()
            }
            if metadata:
                s3_metadata.update(metadata)
            
            # Guess content type
            content_type, _ = mimetypes.guess_type(str(file_path))
            
            # Upload file
            self.client.upload_file(
                str(file_path),
                self.bucket,
                key,
                ExtraArgs={
                    'Metadata': s3_metadata,
                    'ContentType': content_type or 'application/octet-stream'
                }
            )
            
            # Get object details
            response = self.client.head_object(
                Bucket=self.bucket,
                Key=key
            )
            
            return {
                'path': key,
                'hash': file_hash,
                'size': response['ContentLength'],
                'version': response.get('VersionId'),
                'metadata': s3_metadata
            }
            
        except Exception as e:
            logger.error(f"Store operation failed: {e}")
            raise
    
    def retrieve(
        self,
        file_path: Union[str, Path],
        version: Optional[str] = None
    ) -> Dict[str, Any]:
        """Retrieve file from S3."""
        try:
            # Handle both keys and file paths
            if isinstance(file_path, str) and '/' in file_path:
                key = file_path
            else:
                file_path = Path(file_path)
                meta = self.get_metadata(file_path)
                key = meta['path']
            
            # Prepare download args
            kwargs = {
                'Bucket': self.bucket,
                'Key': key
            }
            if version:
                kwargs['VersionId'] = version
            
            # Get object
            response = self.client.get_object(**kwargs)
            
            return {
                'path': key,
                'size': response['ContentLength'],
                'version': response.get('VersionId'),
                'metadata': response.get('Metadata', {}),
                'data': response['Body']
            }
            
        except Exception as e:
            logger.error(f"Retrieve operation failed: {e}")
            raise
    
    def delete(
        self,
        file_path: Union[str, Path],
        version: Optional[str] = None
    ) -> bool:
        """Delete file from S3."""
        try:
            # Get key
            if isinstance(file_path, str) and '/' in file_path:
                key = file_path
            else:
                file_path = Path(file_path)
                meta = self.get_metadata(file_path)
                key = meta['path']
            
            # Prepare delete args
            kwargs = {
                'Bucket': self.bucket,
                'Key': key
            }
            if version:
                kwargs['VersionId'] = version
            
            # Delete object
            self.client.delete_object(**kwargs)
            return True
            
        except Exception as e:
            logger.error(f"Delete operation failed: {e}")
            raise
    
    def list_files(
        self,
        prefix: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List files in S3 bucket."""
        try:
            results = []
            paginator = self.client.get_paginator('list_objects_v2')
            
            # List objects
            for page in paginator.paginate(
                Bucket=self.bucket,
                Prefix=prefix
            ):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Get object metadata
                        response = self.client.head_object(
                            Bucket=self.bucket,
                            Key=obj['Key']
                        )
                        
                        results.append({
                            'path': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'version': response.get('VersionId'),
                            'metadata': response.get('Metadata', {})
                        })
            
            return results
            
        except Exception as e:
            logger.error(f"List operation failed: {e}")
            raise
    
    def get_metadata(
        self,
        file_path: Union[str, Path]
    ) -> Dict[str, Any]:
        """Get file metadata from S3."""
        try:
            # Get key
            if isinstance(file_path, str) and '/' in file_path:
                key = file_path
            else:
                file_path = Path(file_path)
                file_hash = self._calculate_hash(file_path)
                key = f"{file_hash[:2]}/{file_hash[2:4]}/{file_hash}"
            
            # Get object metadata
            response = self.client.head_object(
                Bucket=self.bucket,
                Key=key
            )
            
            return {
                'path': key,
                'size': response['ContentLength'],
                'last_modified': response['LastModified'].isoformat(),
                'version': response.get('VersionId'),
                'metadata': response.get('Metadata', {})
            }
            
        except Exception as e:
            logger.error(f"Metadata retrieval failed: {e}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Get S3 bucket statistics."""
        try:
            total_size = 0
            total_files = 0
            versions = 0
            
            # Count objects and size
            paginator = self.client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket):
                if 'Contents' in page:
                    total_files += len(page['Contents'])
                    total_size += sum(
                        obj['Size'] for obj in page['Contents']
                    )
            
            # Count versions if enabled
            try:
                paginator = self.client.get_paginator(
                    'list_object_versions'
                )
                for page in paginator.paginate(Bucket=self.bucket):
                    if 'Versions' in page:
                        versions += len(page['Versions'])
            except ClientError:
                # Versioning might not be enabled
                pass
            
            return {
                'total_size': total_size,
                'file_count': total_files,
                'version_count': versions,
                'bucket': self.bucket
            }
            
        except Exception as e:
            logger.error(f"Stats retrieval failed: {e}")
            raise
    
    def close(self):
        """Clean up resources."""
        self.executor.shutdown()
    
    def _ensure_bucket(self):
        """Ensure bucket exists and is accessible."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.client.create_bucket(Bucket=self.bucket)
            else:
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