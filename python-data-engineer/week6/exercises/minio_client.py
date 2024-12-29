"""
MinIO Client
-----------

PURPOSE:
Implement MinIO operations with:
1. Server Operations
   - Connection management
   - Health monitoring
   - Policy configuration

2. Object Operations
   - Data management
   - Bucket operations
   - Access control

3. Security
   - Authentication
   - Encryption
   - Access policies
"""

from minio import Minio
from minio.error import S3Error
import logging
from typing import Dict, List, Any, Optional, Union, BinaryIO
from pathlib import Path
import json
from datetime import datetime, timedelta
import io
import hashlib
import tempfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MinioClient:
    """MinIO operations handler."""
    
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = True,
        region: Optional[str] = None
    ):
        """Initialize MinIO client."""
        try:
            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
                region=region
            )
            logger.info(f"Connected to MinIO: {endpoint}")
            
        except Exception as e:
            logger.error(f"MinIO connection error: {e}")
            raise
    
    def create_bucket(
        self,
        bucket_name: str,
        location: str = 'us-east-1'
    ) -> bool:
        """Create MinIO bucket."""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(
                    bucket_name,
                    location=location
                )
                logger.info(f"Created bucket: {bucket_name}")
            return True
            
        except S3Error as e:
            logger.error(f"Bucket creation error: {e}")
            raise
    
    def list_buckets(self) -> List[Dict[str, Any]]:
        """List all buckets."""
        try:
            return [
                {
                    'name': bucket.name,
                    'creation_date': bucket.creation_date
                }
                for bucket in self.client.list_buckets()
            ]
            
        except S3Error as e:
            logger.error(f"List buckets error: {e}")
            raise
    
    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: Union[str, Path],
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Upload file to MinIO."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            # Calculate file hash
            file_hash = hashlib.md5()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    file_hash.update(chunk)
            
            # Upload file
            result = self.client.fput_object(
                bucket_name,
                object_name,
                str(file_path),
                content_type=content_type,
                metadata=metadata
            )
            
            upload_info = {
                'bucket_name': bucket_name,
                'object_name': object_name,
                'etag': result.etag,
                'version_id': result.version_id,
                'md5': file_hash.hexdigest()
            }
            
            logger.info(
                f"Uploaded {file_path} to "
                f"{bucket_name}/{object_name}"
            )
            return upload_info
            
        except S3Error as e:
            logger.error(f"Upload error: {e}")
            raise
    
    def download_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: Union[str, Path],
        version_id: Optional[str] = None
    ) -> bool:
        """Download file from MinIO."""
        try:
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.client.fget_object(
                bucket_name,
                object_name,
                str(file_path),
                version_id=version_id
            )
            
            logger.info(
                f"Downloaded {bucket_name}/{object_name} "
                f"to {file_path}"
            )
            return True
            
        except S3Error as e:
            logger.error(f"Download error: {e}")
            raise
    
    def get_presigned_url(
        self,
        bucket_name: str,
        object_name: str,
        expires: int = 3600,
        response_headers: Optional[Dict[str, str]] = None
    ) -> str:
        """Generate presigned URL for object."""
        try:
            url = self.client.presigned_get_object(
                bucket_name,
                object_name,
                expires=timedelta(seconds=expires),
                response_headers=response_headers
            )
            
            logger.info(
                f"Generated presigned URL for "
                f"{bucket_name}/{object_name}"
            )
            return url
            
        except S3Error as e:
            logger.error(f"Presigned URL error: {e}")
            raise
    
    def set_bucket_policy(
        self,
        bucket_name: str,
        policy: Dict[str, Any]
    ) -> bool:
        """Set bucket policy."""
        try:
            self.client.set_bucket_policy(
                bucket_name,
                json.dumps(policy)
            )
            
            logger.info(
                f"Set policy for bucket: {bucket_name}"
            )
            return True
            
        except S3Error as e:
            logger.error(f"Policy setting error: {e}")
            raise
    
    def enable_bucket_versioning(
        self,
        bucket_name: str
    ) -> bool:
        """Enable bucket versioning."""
        try:
            self.client.set_bucket_versioning(
                bucket_name,
                True
            )
            
            logger.info(
                f"Enabled versioning for {bucket_name}"
            )
            return True
            
        except S3Error as e:
            logger.error(f"Versioning error: {e}")
            raise
    
    def list_objects(
        self,
        bucket_name: str,
        prefix: Optional[str] = None,
        recursive: bool = True
    ) -> List[Dict[str, Any]]:
        """List objects in bucket."""
        try:
            objects = self.client.list_objects(
                bucket_name,
                prefix=prefix,
                recursive=recursive
            )
            
            return [
                {
                    'object_name': obj.object_name,
                    'size': obj.size,
                    'last_modified': obj.last_modified,
                    'etag': obj.etag,
                    'content_type': obj.content_type
                }
                for obj in objects
            ]
            
        except S3Error as e:
            logger.error(f"List objects error: {e}")
            raise
    
    def copy_object(
        self,
        source_bucket: str,
        source_object: str,
        dest_bucket: str,
        dest_object: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Copy object between buckets."""
        try:
            result = self.client.copy_object(
                dest_bucket,
                dest_object,
                f"{source_bucket}/{source_object}",
                metadata=metadata
            )
            
            logger.info(
                f"Copied {source_bucket}/{source_object} to "
                f"{dest_bucket}/{dest_object}"
            )
            
            return {
                'etag': result.etag,
                'version_id': result.version_id
            }
            
        except S3Error as e:
            logger.error(f"Copy error: {e}")
            raise
    
    def get_bucket_encryption(
        self,
        bucket_name: str
    ) -> Dict[str, Any]:
        """Get bucket encryption settings."""
        try:
            config = self.client.get_bucket_encryption(
                bucket_name
            )
            
            return {
                'algorithm': config.rule.apply_server_side_encryption_by_default.sse_algorithm,
                'kms_master_key_id': config.rule.apply_server_side_encryption_by_default.kms_master_key_id
            }
            
        except S3Error as e:
            logger.error(f"Encryption config error: {e}")
            raise
    
    def set_bucket_encryption(
        self,
        bucket_name: str,
        sse_algorithm: str = 'AES256',
        kms_master_key_id: Optional[str] = None
    ) -> bool:
        """Set bucket encryption."""
        try:
            config = {
                'Rule': {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': sse_algorithm
                    }
                }
            }
            
            if kms_master_key_id:
                config['Rule']['ApplyServerSideEncryptionByDefault']['KMSMasterKeyID'] = \
                    kms_master_key_id
            
            self.client.set_bucket_encryption(
                bucket_name,
                config
            )
            
            logger.info(
                f"Set encryption for bucket: {bucket_name}"
            )
            return True
            
        except S3Error as e:
            logger.error(f"Set encryption error: {e}")
            raise

def main():
    """Example usage."""
    # Initialize MinIO client
    minio = MinioClient(
        endpoint='play.min.io',
        access_key='YOUR_ACCESS_KEY',
        secret_key='YOUR_SECRET_KEY',
        secure=True
    )
    
    try:
        # Create bucket
        minio.create_bucket('test-bucket')
        
        # Enable versioning
        minio.enable_bucket_versioning('test-bucket')
        
        # Set bucket policy
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": ["s3:GetObject"],
                    "Resource": ["arn:aws:s3:::test-bucket/*"]
                }
            ]
        }
        minio.set_bucket_policy('test-bucket', policy)
        
        # Upload file
        result = minio.upload_file(
            'test-bucket',
            'test.txt',
            'local_file.txt',
            metadata={'purpose': 'testing'}
        )
        print(f"Upload result: {json.dumps(result, indent=2)}")
        
        # Generate presigned URL
        url = minio.get_presigned_url(
            'test-bucket',
            'test.txt',
            expires=3600
        )
        print(f"Presigned URL: {url}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == '__main__':
    main() 