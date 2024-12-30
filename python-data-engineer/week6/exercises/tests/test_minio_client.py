"""
MinIO Client Tests
----------------

Tests MinIO operations with:
1. Server operations
2. Object management
3. Security features
4. Error handling
"""

import unittest
from unittest.mock import Mock, patch, call
from minio import Minio
from minio.error import S3Error
from pathlib import Path
import json
from datetime import datetime, timedelta
import io
import hashlib
from minio_client import MinioClient

class TestMinioClient(unittest.TestCase):
    """Test cases for MinIO operations."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock MinIO client
        self.mock_minio = Mock(spec=Minio)
        with patch('minio.Minio', return_value=self.mock_minio):
            self.client = MinioClient(
                endpoint='localhost:9000',
                access_key='test_key',
                secret_key='test_secret'
            )
    
    def test_create_bucket(self):
        """Test bucket creation."""
        # Mock bucket doesn't exist
        self.mock_minio.bucket_exists.return_value = False
        
        # Test creation
        result = self.client.create_bucket(
            'test-bucket',
            location='us-east-1'
        )
        
        self.assertTrue(result)
        self.mock_minio.make_bucket.assert_called_with(
            'test-bucket',
            location='us-east-1'
        )
        
        # Test bucket already exists
        self.mock_minio.bucket_exists.return_value = True
        result = self.client.create_bucket('test-bucket')
        self.assertTrue(result)
        self.mock_minio.make_bucket.assert_called_once()
    
    def test_list_buckets(self):
        """Test bucket listing."""
        mock_bucket1 = Mock()
        mock_bucket1.name = 'bucket1'
        mock_bucket1.creation_date = datetime.now()
        
        mock_bucket2 = Mock()
        mock_bucket2.name = 'bucket2'
        mock_bucket2.creation_date = datetime.now()
        
        self.mock_minio.list_buckets.return_value = [
            mock_bucket1,
            mock_bucket2
        ]
        
        buckets = self.client.list_buckets()
        
        self.assertEqual(len(buckets), 2)
        self.assertEqual(buckets[0]['name'], 'bucket1')
        self.assertTrue('creation_date' in buckets[0])
    
    def test_upload_file(self):
        """Test file upload."""
        # Mock file operations
        mock_result = Mock()
        mock_result.etag = 'test-etag'
        mock_result.version_id = 'v1'
        
        self.mock_minio.fput_object.return_value = mock_result
        
        with patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open := Mock()), \
             patch('hashlib.md5') as mock_md5:
            
            # Mock MD5 calculation
            mock_md5_instance = Mock()
            mock_md5_instance.hexdigest.return_value = 'test-md5'
            mock_md5.return_value = mock_md5_instance
            
            # Test upload
            result = self.client.upload_file(
                'test-bucket',
                'test.txt',
                'local/test.txt',
                content_type='text/plain',
                metadata={'purpose': 'test'}
            )
            
            self.assertEqual(result['etag'], 'test-etag')
            self.assertEqual(result['version_id'], 'v1')
            self.assertEqual(result['md5'], 'test-md5')
            
            self.mock_minio.fput_object.assert_called_with(
                'test-bucket',
                'test.txt',
                'local/test.txt',
                content_type='text/plain',
                metadata={'purpose': 'test'}
            )
    
    def test_download_file(self):
        """Test file download."""
        with patch('pathlib.Path.parent.mkdir'):
            result = self.client.download_file(
                'test-bucket',
                'test.txt',
                'local/test.txt'
            )
            
            self.assertTrue(result)
            self.mock_minio.fget_object.assert_called_with(
                'test-bucket',
                'test.txt',
                'local/test.txt',
                version_id=None
            )
    
    def test_get_presigned_url(self):
        """Test presigned URL generation."""
        self.mock_minio.presigned_get_object.return_value = \
            'https://test-url'
        
        url = self.client.get_presigned_url(
            'test-bucket',
            'test.txt',
            expires=3600
        )
        
        self.assertEqual(url, 'https://test-url')
        self.mock_minio.presigned_get_object.assert_called_with(
            'test-bucket',
            'test.txt',
            expires=timedelta(seconds=3600),
            response_headers=None
        )
    
    def test_set_bucket_policy(self):
        """Test bucket policy setting."""
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
        
        result = self.client.set_bucket_policy(
            'test-bucket',
            policy
        )
        
        self.assertTrue(result)
        self.mock_minio.set_bucket_policy.assert_called_with(
            'test-bucket',
            json.dumps(policy)
        )
    
    def test_enable_bucket_versioning(self):
        """Test bucket versioning enablement."""
        result = self.client.enable_bucket_versioning(
            'test-bucket'
        )
        
        self.assertTrue(result)
        self.mock_minio.set_bucket_versioning.assert_called_with(
            'test-bucket',
            True
        )
    
    def test_list_objects(self):
        """Test object listing."""
        mock_obj1 = Mock()
        mock_obj1.object_name = 'test1.txt'
        mock_obj1.size = 1024
        mock_obj1.last_modified = datetime.now()
        mock_obj1.etag = 'etag1'
        mock_obj1.content_type = 'text/plain'
        
        mock_obj2 = Mock()
        mock_obj2.object_name = 'test2.txt'
        mock_obj2.size = 2048
        mock_obj2.last_modified = datetime.now()
        mock_obj2.etag = 'etag2'
        mock_obj2.content_type = 'text/plain'
        
        self.mock_minio.list_objects.return_value = [
            mock_obj1,
            mock_obj2
        ]
        
        objects = self.client.list_objects(
            'test-bucket',
            prefix='test'
        )
        
        self.assertEqual(len(objects), 2)
        self.assertEqual(objects[0]['object_name'], 'test1.txt')
        self.assertEqual(objects[1]['object_name'], 'test2.txt')
    
    def test_copy_object(self):
        """Test object copying."""
        mock_result = Mock()
        mock_result.etag = 'new-etag'
        mock_result.version_id = 'v2'
        
        self.mock_minio.copy_object.return_value = mock_result
        
        result = self.client.copy_object(
            'source-bucket',
            'source.txt',
            'dest-bucket',
            'dest.txt',
            metadata={'copied': 'true'}
        )
        
        self.assertEqual(result['etag'], 'new-etag')
        self.assertEqual(result['version_id'], 'v2')
        
        self.mock_minio.copy_object.assert_called_with(
            'dest-bucket',
            'dest.txt',
            'source-bucket/source.txt',
            metadata={'copied': 'true'}
        )
    
    def test_bucket_encryption(self):
        """Test bucket encryption operations."""
        # Test get encryption
        mock_config = Mock()
        mock_config.rule.apply_server_side_encryption_by_default\
            .sse_algorithm = 'AES256'
        mock_config.rule.apply_server_side_encryption_by_default\
            .kms_master_key_id = None
        
        self.mock_minio.get_bucket_encryption.return_value = \
            mock_config
        
        config = self.client.get_bucket_encryption('test-bucket')
        
        self.assertEqual(config['algorithm'], 'AES256')
        self.assertIsNone(config['kms_master_key_id'])
        
        # Test set encryption
        result = self.client.set_bucket_encryption(
            'test-bucket',
            'AES256'
        )
        
        self.assertTrue(result)
        self.mock_minio.set_bucket_encryption.assert_called()
    
    def test_error_handling(self):
        """Test error handling."""
        # Test bucket creation error
        self.mock_minio.make_bucket.side_effect = \
            S3Error('BucketAlreadyExists')
        
        with self.assertRaises(S3Error):
            self.client.create_bucket('test-bucket')
        
        # Test access denied
        self.mock_minio.fput_object.side_effect = \
            S3Error('AccessDenied')
        
        with self.assertRaises(S3Error):
            self.client.upload_file(
                'test-bucket',
                'test.txt',
                'local/test.txt'
            )
    
    def tearDown(self):
        """Clean up test environment."""
        pass

if __name__ == '__main__':
    unittest.main() 