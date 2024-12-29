"""
S3 Operations
-----------

PURPOSE:
Implement AWS S3 operations with:
1. Bucket Management
   - Create/Delete buckets
   - Configure policies
   - Set permissions

2. Object Operations
   - Upload/Download
   - Copy/Move/Delete
   - Versioning

3. Advanced Features
   - Lifecycle policies
   - Event notifications
   - Access logging
"""

import boto3
import logging
from botocore.exceptions import ClientError
from typing import Dict, List, Any, Optional, Union, BinaryIO
from pathlib import Path
import json
from datetime import datetime
import mimetypes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class S3Operations:
    """AWS S3 operations handler."""
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = 'us-east-1',
        endpoint_url: Optional[str] = None
    ):
        """Initialize S3 client."""
        try:
            self.s3 = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
                endpoint_url=endpoint_url
            )
            logger.info("Initialized S3 client")
            
        except Exception as e:
            logger.error(f"S3 client initialization error: {e}")
            raise
    
    def create_bucket(
        self,
        bucket_name: str,
        region: Optional[str] = None
    ) -> bool:
        """Create S3 bucket."""
        try:
            location = {'LocationConstraint': region} \
                if region else {}
            
            self.s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location
            )
            
            logger.info(f"Created bucket: {bucket_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Bucket creation error: {e}")
            raise
    
    def delete_bucket(
        self,
        bucket_name: str,
        force: bool = False
    ) -> bool:
        """Delete S3 bucket."""
        try:
            if force:
                self.delete_all_objects(bucket_name)
            
            self.s3.delete_bucket(Bucket=bucket_name)
            logger.info(f"Deleted bucket: {bucket_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Bucket deletion error: {e}")
            raise
    
    def list_buckets(self) -> List[Dict[str, Any]]:
        """List all S3 buckets."""
        try:
            response = self.s3.list_buckets()
            return [
                {
                    'name': bucket['Name'],
                    'creation_date': bucket['CreationDate']
                }
                for bucket in response['Buckets']
            ]
            
        except ClientError as e:
            logger.error(f"List buckets error: {e}")
            raise
    
    def upload_file(
        self,
        file_path: Union[str, Path],
        bucket_name: str,
        object_name: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Upload file to S3."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            object_name = object_name or file_path.name
            
            # Determine content type
            content_type = mimetypes.guess_type(file_path)[0]
            extra_args = extra_args or {}
            if content_type:
                extra_args['ContentType'] = content_type
            
            self.s3.upload_file(
                str(file_path),
                bucket_name,
                object_name,
                ExtraArgs=extra_args
            )
            
            logger.info(
                f"Uploaded {file_path} to "
                f"{bucket_name}/{object_name}"
            )
            return True
            
        except ClientError as e:
            logger.error(f"Upload error: {e}")
            raise
    
    def download_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: Union[str, Path]
    ) -> bool:
        """Download file from S3."""
        try:
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.s3.download_file(
                bucket_name,
                object_name,
                str(file_path)
            )
            
            logger.info(
                f"Downloaded {bucket_name}/{object_name} "
                f"to {file_path}"
            )
            return True
            
        except ClientError as e:
            logger.error(f"Download error: {e}")
            raise
    
    def delete_object(
        self,
        bucket_name: str,
        object_name: str,
        version_id: Optional[str] = None
    ) -> bool:
        """Delete object from S3."""
        try:
            kwargs = {'Bucket': bucket_name, 'Key': object_name}
            if version_id:
                kwargs['VersionId'] = version_id
            
            self.s3.delete_object(**kwargs)
            logger.info(
                f"Deleted {object_name} from {bucket_name}"
            )
            return True
            
        except ClientError as e:
            logger.error(f"Delete error: {e}")
            raise
    
    def delete_all_objects(
        self,
        bucket_name: str
    ) -> bool:
        """Delete all objects in bucket."""
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket_name):
                if 'Contents' in page:
                    objects = [
                        {'Key': obj['Key']}
                        for obj in page['Contents']
                    ]
                    
                    self.s3.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': objects}
                    )
            
            logger.info(
                f"Deleted all objects from {bucket_name}"
            )
            return True
            
        except ClientError as e:
            logger.error(f"Bulk delete error: {e}")
            raise
    
    def enable_versioning(
        self,
        bucket_name: str
    ) -> bool:
        """Enable versioning for bucket."""
        try:
            self.s3.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={
                    'Status': 'Enabled'
                }
            )
            
            logger.info(
                f"Enabled versioning for {bucket_name}"
            )
            return True
            
        except ClientError as e:
            logger.error(f"Versioning error: {e}")
            raise
    
    def set_lifecycle_policy(
        self,
        bucket_name: str,
        rules: List[Dict[str, Any]]
    ) -> bool:
        """Set lifecycle policy for bucket."""
        try:
            self.s3.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration={'Rules': rules}
            )
            
            logger.info(
                f"Set lifecycle policy for {bucket_name}"
            )
            return True
            
        except ClientError as e:
            logger.error(f"Lifecycle policy error: {e}")
            raise
    
    def get_object_versions(
        self,
        bucket_name: str,
        object_name: str
    ) -> List[Dict[str, Any]]:
        """Get version history of object."""
        try:
            response = self.s3.list_object_versions(
                Bucket=bucket_name,
                Prefix=object_name
            )
            
            versions = []
            if 'Versions' in response:
                versions.extend([
                    {
                        'version_id': v['VersionId'],
                        'last_modified': v['LastModified'],
                        'size': v['Size'],
                        'is_latest': v['IsLatest']
                    }
                    for v in response['Versions']
                    if v['Key'] == object_name
                ])
            
            return versions
            
        except ClientError as e:
            logger.error(f"Version listing error: {e}")
            raise
    
    def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
        version_id: Optional[str] = None
    ) -> bool:
        """Copy object within or between buckets."""
        try:
            source = {
                'Bucket': source_bucket,
                'Key': source_key
            }
            if version_id:
                source['VersionId'] = version_id
            
            self.s3.copy_object(
                CopySource=source,
                Bucket=dest_bucket,
                Key=dest_key
            )
            
            logger.info(
                f"Copied {source_bucket}/{source_key} to "
                f"{dest_bucket}/{dest_key}"
            )
            return True
            
        except ClientError as e:
            logger.error(f"Copy error: {e}")
            raise

def main():
    """Example usage."""
    # Initialize with AWS credentials
    s3 = S3Operations(
        aws_access_key_id='YOUR_ACCESS_KEY',
        aws_secret_access_key='YOUR_SECRET_KEY',
        region_name='us-east-1'
    )
    
    try:
        # Create bucket
        s3.create_bucket('test-bucket')
        
        # Enable versioning
        s3.enable_versioning('test-bucket')
        
        # Upload file
        s3.upload_file(
            'test.txt',
            'test-bucket',
            'folder/test.txt'
        )
        
        # Set lifecycle policy
        lifecycle_rules = [{
            'ID': 'Move to IA after 30 days',
            'Status': 'Enabled',
            'Transition': {
                'Days': 30,
                'StorageClass': 'STANDARD_IA'
            }
        }]
        s3.set_lifecycle_policy('test-bucket', lifecycle_rules)
        
        # List versions
        versions = s3.get_object_versions(
            'test-bucket',
            'folder/test.txt'
        )
        print(f"Object versions: {json.dumps(versions, indent=2)}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == '__main__':
    main() 