"""
Object storage management.
"""

from typing import Dict, Any, Optional, BinaryIO
import logging
from minio import Minio
from datetime import timedelta
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ObjectManager:
    """Object storage management."""
    
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = True
    ):
        """Initialize manager."""
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.buckets = {}
    
    def create_bucket(
        self,
        name: str,
        location: str = "us-east-1"
    ) -> None:
        """
        Create bucket.
        
        Args:
            name: Bucket name
            location: Region
        """
        try:
            if not self.client.bucket_exists(name):
                self.client.make_bucket(name, location)
                
                # Store metadata
                self.buckets[name] = {
                    'location': location,
                    'created_at': datetime.now().isoformat()
                }
                
                logger.info(f"Created bucket: {name}")
            
        except Exception as e:
            logger.error(f"Failed to create bucket: {e}")
            raise
    
    def upload_file(
        self,
        bucket: str,
        object_name: str,
        file_path: str,
        content_type: Optional[str] = None
    ) -> None:
        """
        Upload file.
        
        Args:
            bucket: Bucket name
            object_name: Object name
            file_path: File path
            content_type: Content type
        """
        try:
            self.client.fput_object(
                bucket,
                object_name,
                file_path,
                content_type=content_type
            )
            
            logger.info(
                f"Uploaded {file_path} to {bucket}/{object_name}"
            )
            
        except Exception as e:
            logger.error(f"Failed to upload file: {e}")
            raise
    
    def download_file(
        self,
        bucket: str,
        object_name: str,
        file_path: str
    ) -> None:
        """
        Download file.
        
        Args:
            bucket: Bucket name
            object_name: Object name
            file_path: File path
        """
        try:
            self.client.fget_object(
                bucket,
                object_name,
                file_path
            )
            
            logger.info(
                f"Downloaded {bucket}/{object_name} to {file_path}"
            )
            
        except Exception as e:
            logger.error(f"Failed to download file: {e}")
            raise
    
    def get_presigned_url(
        self,
        bucket: str,
        object_name: str,
        expires: timedelta = timedelta(hours=1)
    ) -> str:
        """
        Get presigned URL.
        
        Args:
            bucket: Bucket name
            object_name: Object name
            expires: Expiration time
        
        Returns:
            Presigned URL
        """
        try:
            url = self.client.presigned_get_object(
                bucket,
                object_name,
                expires=expires
            )
            
            return url
            
        except Exception as e:
            logger.error(f"Failed to get presigned URL: {e}")
            raise
    
    def list_objects(
        self,
        bucket: str,
        prefix: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List objects.
        
        Args:
            bucket: Bucket name
            prefix: Object prefix
        
        Returns:
            List of objects
        """
        try:
            objects = self.client.list_objects(
                bucket,
                prefix=prefix
            )
            
            return [
                {
                    'name': obj.object_name,
                    'size': obj.size,
                    'last_modified': obj.last_modified
                }
                for obj in objects
            ]
            
        except Exception as e:
            logger.error(f"Failed to list objects: {e}")
            raise 