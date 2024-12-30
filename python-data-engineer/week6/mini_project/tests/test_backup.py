"""
Backup Tests
----------

Test cases for backup system:
1. Backup strategies
2. Backup scheduling
3. Recovery operations
4. Retention policies
"""

import pytest
from datetime import datetime, timedelta
import asyncio
from typing import Dict, Any
import json
from pathlib import Path
import hashlib

@pytest.mark.asyncio
async def test_backup_strategy(
    backup_manager,
    storage_backends: Dict[str, Any],
    test_file: Path
):
    """Test backup strategy operations."""
    # Store test file in each backend
    for storage in storage_backends.values():
        await asyncio.to_thread(
            storage.store,
            test_file,
            {'purpose': 'backup_test'}
        )
    
    # Create backup using default strategy
    strategy_name = next(iter(backup_manager.strategies))
    result = await backup_manager.create_backup(
        strategy_name
    )
    
    # Verify backup result
    assert result['success']
    assert 'backup_id' in result
    assert 'file_count' in result
    assert 'total_size' in result
    
    # Verify backup can be listed
    backups = backup_manager.list_backups(
        strategy_name=strategy_name
    )
    assert len(backups[strategy_name]) > 0
    assert result['backup_id'] in [
        b['backup_id']
        for b in backups[strategy_name]
    ]
    
    # Test backup verification
    verify_result = await backup_manager.verify_backup(
        result['backup_id'],
        strategy_name
    )
    assert verify_result['verified'] > 0
    assert verify_result['failed'] == 0

@pytest.mark.asyncio
async def test_backup_restore(
    backup_manager,
    storage_backends: Dict[str, Any],
    test_file: Path,
    tmp_path: Path
):
    """Test backup restoration."""
    # Create backup
    strategy_name = next(iter(backup_manager.strategies))
    backup_result = await backup_manager.create_backup(
        strategy_name
    )
    
    # Delete original files
    for storage in storage_backends.values():
        await asyncio.to_thread(
            storage.delete,
            test_file
        )
    
    # Restore from backup
    restore_path = tmp_path / "restore"
    restore_path.mkdir()
    
    restore_result = await backup_manager.restore_backup(
        backup_result['backup_id'],
        strategy_name,
        target_path=restore_path
    )
    
    # Verify restoration
    assert restore_result['success']
    assert restore_result['restored_files'] > 0
    assert (restore_path / test_file.name).exists()
    assert (
        (restore_path / test_file.name).read_bytes() ==
        test_file.read_bytes()
    )

@pytest.mark.asyncio
async def test_backup_scheduling(
    backup_manager,
    storage_backends: Dict[str, Any]
):
    """Test backup scheduling."""
    # Get initial schedule
    initial_jobs = backup_manager.get_schedule()
    
    # Add test backup job
    backup_manager.scheduler.add_job(
        name='test_backup',
        schedule='*/5 * * * *',  # Every 5 minutes
        func=backup_manager.create_backup,
        args=['daily']
    )
    
    # Verify job was added
    current_jobs = backup_manager.get_schedule()
    assert len(current_jobs) == len(initial_jobs) + 1
    assert any(
        job['name'] == 'test_backup'
        for job in current_jobs
    )
    
    # Start scheduler briefly
    backup_manager.start()
    await asyncio.sleep(2)
    backup_manager.stop()

@pytest.mark.asyncio
async def test_backup_retention(
    backup_manager,
    storage_backends: Dict[str, Any],
    test_file: Path
):
    """Test backup retention policies."""
    strategy_name = next(iter(backup_manager.strategies))
    strategy = backup_manager.strategies[strategy_name]
    
    # Create multiple backups
    backup_ids = []
    for i in range(strategy.retention + 2):
        result = await backup_manager.create_backup(
            strategy_name
        )
        backup_ids.append(result['backup_id'])
        await asyncio.sleep(1)  # Ensure unique timestamps
    
    # Clean up old backups
    deleted = await asyncio.to_thread(
        strategy.cleanup_old_backups,
        storage_backends[strategy.destination]
    )
    
    # Verify retention
    assert len(deleted) > 0
    current_backups = backup_manager.list_backups(
        strategy_name=strategy_name
    )[strategy_name]
    assert len(current_backups) <= strategy.retention

@pytest.mark.asyncio
async def test_backup_error_handling(
    backup_manager,
    storage_backends: Dict[str, Any]
):
    """Test backup error handling."""
    strategy_name = next(iter(backup_manager.strategies))
    
    # Test non-existent backup restore
    with pytest.raises(Exception):
        await backup_manager.restore_backup(
            'nonexistent_backup_id',
            strategy_name
        )
    
    # Test invalid strategy
    with pytest.raises(KeyError):
        await backup_manager.create_backup(
            'invalid_strategy'
        )
    
    # Test backup with no files
    result = await backup_manager.create_backup(
        strategy_name,
        files=[]
    )
    assert not result['success']
    assert 'error' in result

@pytest.mark.asyncio
async def test_concurrent_backups(
    backup_manager,
    storage_backends: Dict[str, Any],
    tmp_path: Path
):
    """Test concurrent backup operations."""
    # Create test files
    files = []
    for i in range(5):
        file_path = tmp_path / f"test_{i}.txt"
        file_path.write_text(f"Test content {i}")
        files.append(file_path)
    
    # Store files
    for storage in storage_backends.values():
        for file_path in files:
            await asyncio.to_thread(
                storage.store,
                file_path,
                {'index': files.index(file_path)}
            )
    
    # Run concurrent backups
    strategy_name = next(iter(backup_manager.strategies))
    tasks = [
        asyncio.create_task(
            backup_manager.create_backup(strategy_name)
        )
        for _ in range(3)
    ]
    
    results = await asyncio.gather(*tasks)
    
    # Verify results
    assert all(r['success'] for r in results)
    assert len(set(r['backup_id'] for r in results)) == 3

@pytest.mark.asyncio
async def test_backup_metadata(
    backup_manager,
    storage_backends: Dict[str, Any],
    test_file: Path
):
    """Test backup metadata handling."""
    strategy_name = next(iter(backup_manager.strategies))
    
    # Create backup with metadata
    metadata = {
        'description': 'Test backup',
        'tags': ['test', 'metadata'],
        'custom_field': 123
    }
    
    result = await backup_manager.create_backup(
        strategy_name,
        metadata=metadata
    )
    
    # Verify metadata in backup listing
    backups = backup_manager.list_backups(
        strategy_name=strategy_name
    )[strategy_name]
    
    backup = next(
        b for b in backups
        if b['backup_id'] == result['backup_id']
    )
    
    assert 'metadata' in backup
    assert backup['metadata']['description'] == 'Test backup'
    assert 'test' in backup['metadata']['tags'] 