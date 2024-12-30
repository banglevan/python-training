"""
Tiering Tests
-----------

Test cases for storage tiering:
1. Policy evaluation
2. Data movement
3. Access patterns
4. Performance optimization
"""

import pytest
from datetime import datetime, timedelta
import asyncio
from typing import Dict, Any
import json
from pathlib import Path
import time

@pytest.mark.asyncio
async def test_policy_evaluation(
    tiering_manager,
    test_file: Path
):
    """Test tiering policy evaluation."""
    # Create test metadata
    metadata = {
        'path': str(test_file),
        'size': test_file.stat().st_size,
        'timestamp': datetime.now().isoformat(),
        'access_count': 15,
        'last_access': datetime.now().isoformat()
    }
    
    # Test policy matching
    tier = tiering_manager.determine_tier(
        test_file,
        metadata
    )
    
    # Should match 'hot' tier due to high access count
    assert tier == 'hot'
    
    # Test with different access patterns
    metadata['access_count'] = 3
    metadata['timestamp'] = (
        datetime.now() - timedelta(days=10)
    ).isoformat()
    
    tier = tiering_manager.determine_tier(
        test_file,
        metadata
    )
    
    # Should match 'warm' tier due to low access and age
    assert tier == 'warm'

@pytest.mark.asyncio
async def test_data_movement(
    tiering_manager,
    storage_backends: Dict[str, Any],
    test_file: Path
):
    """Test data movement between tiers."""
    # Store in initial tier
    initial_tier = 'hot'
    await tiering_manager.store_file(
        test_file,
        initial_tier
    )
    
    # Verify file location
    location = await tiering_manager.get_file_location(
        test_file
    )
    assert location['current_tier'] == initial_tier
    
    # Move to different tier
    target_tier = 'warm'
    await tiering_manager.move_file(
        test_file,
        target_tier
    )
    
    # Verify new location
    location = await tiering_manager.get_file_location(
        test_file
    )
    assert location['current_tier'] == target_tier
    
    # Verify file is accessible
    content = await tiering_manager.retrieve_file(
        test_file
    )
    assert content['data'].read() == test_file.read_bytes()

@pytest.mark.asyncio
async def test_access_tracking(
    tiering_manager,
    test_file: Path
):
    """Test access pattern tracking."""
    # Store file
    await tiering_manager.store_file(
        test_file,
        'warm'
    )
    
    # Simulate multiple accesses
    for _ in range(5):
        await tiering_manager.retrieve_file(test_file)
        await asyncio.sleep(0.1)
    
    # Get access statistics
    stats = await tiering_manager.get_access_stats(
        test_file
    )
    
    assert stats['access_count'] >= 5
    assert 'last_access' in stats
    assert 'first_access' in stats

@pytest.mark.asyncio
async def test_automatic_tiering(
    tiering_manager,
    storage_backends: Dict[str, Any],
    test_file: Path
):
    """Test automatic tiering based on access patterns."""
    # Store in warm tier
    await tiering_manager.store_file(
        test_file,
        'warm'
    )
    
    # Simulate frequent access
    for _ in range(10):
        await tiering_manager.retrieve_file(test_file)
        await asyncio.sleep(0.1)
    
    # Run tiering optimization
    await tiering_manager.optimize_placement()
    
    # Verify file was moved to hot tier
    location = await tiering_manager.get_file_location(
        test_file
    )
    assert location['current_tier'] == 'hot'

@pytest.mark.asyncio
async def test_concurrent_tiering(
    tiering_manager,
    tmp_path: Path
):
    """Test concurrent tiering operations."""
    # Create test files
    files = []
    for i in range(5):
        file_path = tmp_path / f"test_{i}.txt"
        file_path.write_text(f"Test content {i}")
        files.append(file_path)
    
    # Store files concurrently
    tasks = [
        asyncio.create_task(
            tiering_manager.store_file(
                file_path,
                'warm'
            )
        )
        for file_path in files
    ]
    
    await asyncio.gather(*tasks)
    
    # Move files concurrently
    tasks = [
        asyncio.create_task(
            tiering_manager.move_file(
                file_path,
                'hot'
            )
        )
        for file_path in files
    ]
    
    await asyncio.gather(*tasks)
    
    # Verify all files moved
    for file_path in files:
        location = await tiering_manager.get_file_location(
            file_path
        )
        assert location['current_tier'] == 'hot'

@pytest.mark.asyncio
async def test_tiering_metadata(
    tiering_manager,
    test_file: Path
):
    """Test tiering metadata management."""
    # Store with metadata
    metadata = {
        'importance': 'high',
        'project': 'test',
        'retention': '90d'
    }
    
    await tiering_manager.store_file(
        test_file,
        'hot',
        metadata=metadata
    )
    
    # Retrieve metadata
    stored_metadata = await tiering_manager.get_metadata(
        test_file
    )
    
    assert stored_metadata['importance'] == 'high'
    assert stored_metadata['project'] == 'test'

@pytest.mark.asyncio
async def test_tiering_cleanup(
    tiering_manager,
    test_file: Path
):
    """Test tiering cleanup operations."""
    # Store file
    await tiering_manager.store_file(
        test_file,
        'warm'
    )
    
    # Mark as expired
    await tiering_manager.update_metadata(
        test_file,
        {
            'expiration': (
                datetime.now() - timedelta(days=1)
            ).isoformat()
        }
    )
    
    # Run cleanup
    cleaned = await tiering_manager.cleanup_expired()
    
    # Verify file was cleaned up
    assert test_file.name in [
        Path(f).name for f in cleaned
    ]
    
    with pytest.raises(FileNotFoundError):
        await tiering_manager.get_file_location(
            test_file
        )

@pytest.mark.asyncio
async def test_tiering_performance(
    tiering_manager,
    test_file: Path
):
    """Test tiering performance metrics."""
    # Store file
    start_time = time.time()
    await tiering_manager.store_file(
        test_file,
        'hot'
    )
    store_time = time.time() - start_time
    
    # Retrieve file
    start_time = time.time()
    await tiering_manager.retrieve_file(test_file)
    retrieve_time = time.time() - start_time
    
    # Get performance metrics
    metrics = await tiering_manager.get_performance_metrics()
    
    assert 'average_store_time' in metrics
    assert 'average_retrieve_time' in metrics
    assert metrics['average_store_time'] > 0
    assert metrics['average_retrieve_time'] > 0

@pytest.mark.asyncio
async def test_tiering_policies_crud(
    tiering_manager
):
    """Test tiering policy CRUD operations."""
    # Create new policy
    new_policy = {
        'storage': 'local',
        'max_age': '14d',
        'min_access': 7,
        'priority': 75
    }
    
    tiering_manager.add_policy(
        'medium',
        new_policy
    )
    
    # Verify policy was added
    assert 'medium' in tiering_manager.policies
    
    # Update policy
    tiering_manager.update_policy(
        'medium',
        {'min_access': 5}
    )
    assert (
        tiering_manager.policies['medium'].min_access == 5
    )
    
    # Delete policy
    tiering_manager.remove_policy('medium')
    assert 'medium' not in tiering_manager.policies 