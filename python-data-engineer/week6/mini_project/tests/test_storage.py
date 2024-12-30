"""
Storage Tests
-----------

Test cases for storage backends:
1. Basic operations
2. Error handling
3. Performance
4. Concurrency
"""

import pytest
from pathlib import Path
import asyncio
from typing import Dict, Any
import hashlib

@pytest.mark.asyncio
async def test_local_storage_basic_ops(
    storage_backends: Dict[str, Any],
    test_file: Path
):
    """Test basic local storage operations."""
    storage = storage_backends['local']
    
    # Test store
    result = await asyncio.to_thread(
        storage.store,
        test_file,
        {'purpose': 'testing'}
    )
    assert result['success']
    assert result['path'] == str(test_file)
    
    # Test retrieve
    content = await asyncio.to_thread(
        storage.retrieve,
        test_file
    )
    assert content['data'].read() == test_file.read_bytes()
    
    # Test metadata
    meta = await asyncio.to_thread(
        storage.get_metadata,
        test_file
    )
    assert meta['purpose'] == 'testing'
    
    # Test delete
    result = await asyncio.to_thread(
        storage.delete,
        test_file
    )
    assert result['success']
    
    # Verify file is gone
    with pytest.raises(FileNotFoundError):
        await asyncio.to_thread(
            storage.retrieve,
            test_file
        )

@pytest.mark.asyncio
async def test_s3_storage_basic_ops(
    storage_backends: Dict[str, Any],
    test_file: Path
):
    """Test basic S3 storage operations."""
    storage = storage_backends['s3']
    
    # Test store
    result = await asyncio.to_thread(
        storage.store,
        test_file,
        {'purpose': 'testing'}
    )
    assert result['success']
    
    # Test retrieve
    content = await asyncio.to_thread(
        storage.retrieve,
        test_file
    )
    assert content['data'].read() == test_file.read_bytes()
    
    # Test metadata
    meta = await asyncio.to_thread(
        storage.get_metadata,
        test_file
    )
    assert meta['purpose'] == 'testing'
    
    # Test delete
    result = await asyncio.to_thread(
        storage.delete,
        test_file
    )
    assert result['success']

@pytest.mark.asyncio
async def test_storage_concurrent_ops(
    storage_backends: Dict[str, Any],
    tmp_path: Path
):
    """Test concurrent storage operations."""
    storage = storage_backends['local']
    files = []
    
    # Create test files
    for i in range(10):
        file_path = tmp_path / f"test_{i}.txt"
        file_path.write_text(f"Test content {i}")
        files.append(file_path)
    
    # Test concurrent store
    tasks = [
        asyncio.create_task(
            asyncio.to_thread(
                storage.store,
                file_path,
                {'index': i}
            )
        )
        for i, file_path in enumerate(files)
    ]
    
    results = await asyncio.gather(*tasks)
    assert all(r['success'] for r in results)
    
    # Test concurrent retrieve
    tasks = [
        asyncio.create_task(
            asyncio.to_thread(
                storage.retrieve,
                file_path
            )
        )
        for file_path in files
    ]
    
    contents = await asyncio.gather(*tasks)
    assert all(
        c['data'].read().decode().startswith('Test content')
        for c in contents
    )
    
    # Test concurrent delete
    tasks = [
        asyncio.create_task(
            asyncio.to_thread(
                storage.delete,
                file_path
            )
        )
        for file_path in files
    ]
    
    results = await asyncio.gather(*tasks)
    assert all(r['success'] for r in results)

@pytest.mark.asyncio
async def test_storage_error_handling(
    storage_backends: Dict[str, Any]
):
    """Test storage error handling."""
    storage = storage_backends['local']
    
    # Test non-existent file
    with pytest.raises(FileNotFoundError):
        await asyncio.to_thread(
            storage.retrieve,
            'nonexistent.txt'
        )
    
    # Test invalid path
    with pytest.raises(ValueError):
        await asyncio.to_thread(
            storage.store,
            '/invalid/path/test.txt',
            {}
        )
    
    # Test metadata validation
    with pytest.raises(ValueError):
        await asyncio.to_thread(
            storage.store,
            'test.txt',
            {'invalid': lambda x: x}  # Non-serializable
        ) 