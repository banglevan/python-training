"""
S3 Operations Tests
----------------

Tests AWS S3 operations with:
1. Bucket operations
2. Object management
3. Versioning
4. Lifecycle policies
"""

import unittest
from unittest.mock import Mock, patch, call
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
import json
from datetime import datetime
from s3_ops import S3Operations

class TestS3Operations(unittest.TestCase):
    """Test cases for S3 operations."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock S3 client
        self.mock_s3 = Mock()
        with patch('boto3.client', return_value=self.mock_s3):
            self.s3_ops = S3Operations(
                aws_access_key_id='test_key',
                aws_secret_access_key='test_secret'
            )
    
    def test_create_bucket(self):
        """Test bucket creation."""
        # Test without region
        self.s3_ops.create_bucket('test-bucket')
        self.mock_s3.create_bucket.assert_called_with(
            Bucket='test-bucket',
            CreateBucketConfiguration={}
        )
        
        # Test with region
        self.s3_ops.create_bucket(
            'test-bucket',
            region='us-west-2'
        )
        self.mock_s3.create_bucket.assert_called_with(
            Bucket='test-bucket',
            CreateBucketConfiguration={
                'LocationConstraint': 'us-west-2'
            }
        )
    
    def test_delete_bucket(self):
        """Test bucket deletion."""
        # Test normal deletion
        self.s3_ops.delete_bucket('test-bucket')
        self.mock_s3.delete_bucket.assert_called_with(
            Bucket='test-bucket'
        )
        
        # Test force deletion
        self.mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'test1.txt'},
                {'Key': 'test2.txt'}
            ]
        }
        
        self.s3_ops.delete_bucket('test-bucket', force=True)
        self.mock_s3.delete_objects.assert_called()
        self.mock_s3.delete_bucket.assert_called()
    
    def test_list_buckets(self):
        """Test bucket listing."""
        mock_response = {
            'Buckets': [
                {
                    'Name': 'bucket1',
                    'CreationDate': datetime.now()
                },
                {
                    'Name': 'bucket2',
                    'CreationDate': datetime.now()
                }
            ]
        }
        
        self.mock_s3.list_buckets.return_value = mock_response
        buckets = self.s3_ops.list_buckets()
        
        self.assertEqual(len(buckets), 2)
        self.assertEqual(buckets[0]['name'], 'bucket1')
        self.assertTrue('creation_date' in buckets[0])
    
    def test_upload_file(self):
        """Test file upload."""
        with patch('pathlib.Path.exists', return_value=True), \
             patch('mimetypes.guess_type',
                   return_value=('text/plain', None)):
            
            # Test basic upload
            self.s3_ops.upload_file(
                'test.txt',
                'test-bucket',
                'folder/test.txt'
            )
            
            self.mock_s3.upload_file.assert_called_with(
                'test.txt',
                'test-bucket',
                'folder/test.txt',
                ExtraArgs={'ContentType': 'text/plain'}
            )
            
            # Test with extra args
            extra_args = {'Metadata': {'purpose': 'test'}}
            self.s3_ops.upload_file(
                'test.txt',
                'test-bucket',
                'test.txt',
                extra_args=extra_args
            )
            
            self.mock_s3.upload_file.assert_called_with(
                'test.txt',
                'test-bucket',
                'test.txt',
                ExtraArgs={
                    'ContentType': 'text/plain',
                    'Metadata': {'purpose': 'test'}
                }
            )
    
    def test_download_file(self):
        """Test file download."""
        with patch('pathlib.Path.parent.mkdir'):
            self.s3_ops.download_file(
                'test-bucket',
                'test.txt',
                'local/test.txt'
            )
            
            self.mock_s3.download_file.assert_called_with(
                'test-bucket',
                'test.txt',
                'local/test.txt'
            )
    
    def test_delete_object(self):
        """Test object deletion."""
        # Test normal deletion
        self.s3_ops.delete_object(
            'test-bucket',
            'test.txt'
        )
        self.mock_s3.delete_object.assert_called_with(
            Bucket='test-bucket',
            Key='test.txt'
        )
        
        # Test version-specific deletion
        self.s3_ops.delete_object(
            'test-bucket',
            'test.txt',
            'v1'
        )
        self.mock_s3.delete_object.assert_called_with(
            Bucket='test-bucket',
            Key='test.txt',
            VersionId='v1'
        )
    
    def test_enable_versioning(self):
        """Test versioning enablement."""
        self.s3_ops.enable_versioning('test-bucket')
        self.mock_s3.put_bucket_versioning.assert_called_with(
            Bucket='test-bucket',
            VersioningConfiguration={'Status': 'Enabled'}
        )
    
    def test_set_lifecycle_policy(self):
        """Test lifecycle policy configuration."""
        rules = [{
            'ID': 'Move to IA',
            'Status': 'Enabled',
            'Transition': {
                'Days': 30,
                'StorageClass': 'STANDARD_IA'
            }
        }]
        
        self.s3_ops.set_lifecycle_policy(
            'test-bucket',
            rules
        )
        
        self.mock_s3.put_bucket_lifecycle_configuration\
            .assert_called_with(
                Bucket='test-bucket',
                LifecycleConfiguration={'Rules': rules}
            )
    
    def test_get_object_versions(self):
        """Test object version listing."""
        mock_response = {
            'Versions': [
                {
                    'Key': 'test.txt',
                    'VersionId': 'v1',
                    'LastModified': datetime.now(),
                    'Size': 1024,
                    'IsLatest': True
                },
                {
                    'Key': 'test.txt',
                    'VersionId': 'v2',
                    'LastModified': datetime.now(),
                    'Size': 1024,
                    'IsLatest': False
                }
            ]
        }
        
        self.mock_s3.list_object_versions\
            .return_value = mock_response
        
        versions = self.s3_ops.get_object_versions(
            'test-bucket',
            'test.txt'
        )
        
        self.assertEqual(len(versions), 2)
        self.assertTrue(versions[0]['is_latest'])
        self.assertFalse(versions[1]['is_latest'])
    
    def test_copy_object(self):
        """Test object copying."""
        self.s3_ops.copy_object(
            'source-bucket',
            'source.txt',
            'dest-bucket',
            'dest.txt'
        )
        
        self.mock_s3.copy_object.assert_called_with(
            CopySource={
                'Bucket': 'source-bucket',
                'Key': 'source.txt'
            },
            Bucket='dest-bucket',
            Key='dest.txt'
        )
    
    def test_error_handling(self):
        """Test error handling."""
        # Test bucket not found
        self.mock_s3.head_bucket.side_effect = \
            ClientError(
                {
                    'Error': {
                        'Code': 'NoSuchBucket',
                        'Message': 'Not Found'
                    }
                },
                'HeadBucket'
            )
        
        with self.assertRaises(ClientError):
            self.s3_ops.delete_bucket('nonexistent-bucket')
        
        # Test access denied
        self.mock_s3.put_bucket_versioning.side_effect = \
            ClientError(
                {
                    'Error': {
                        'Code': 'AccessDenied',
                        'Message': 'Access Denied'
                    }
                },
                'PutBucketVersioning'
            )
        
        with self.assertRaises(ClientError):
            self.s3_ops.enable_versioning('test-bucket')
    
    def tearDown(self):
        """Clean up test environment."""
        pass

if __name__ == '__main__':
    unittest.main() 