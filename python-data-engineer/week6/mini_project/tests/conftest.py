"""
Test Configuration
---------------

Pytest fixtures and configuration for testing:
1. Storage backends
2. Monitoring system
3. Backup system
4. Test data
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import yaml
import os
import asyncio
from typing import Dict, Any

from hybrid_storage.storage import (
    LocalStorage,
    S3Storage,
    HDFSStorage,
    CephStorage
)
from hybrid_storage.monitor import MonitoringSystem
from hybrid_storage.backup import BackupManager
from hybrid_storage.tiering import TieringManager

@pytest.fixture
def test_config() -> Dict[str, Any]:
    """Create test configuration."""
    return {
        'storage': {
            'local': {
                'base_path': '/tmp/hybrid_storage_test',
                'max_size': '1GB',
                'reserved_space': '100MB'
            },
            's3': {
                'endpoint': 'http://localhost:9000',
                'access_key': 'test',
                'secret_key': 'test123',
                'bucket': 'test-bucket'
            }
        },
        'monitoring': {
            'interval': 10,
            'metrics': {
                'retention_days': 7,
                'collection_interval': 30,
                'prometheus_port': 9091
            },
            'alerts': {
                'thresholds': {
                    'storage_usage': 85,
                    'cpu_usage': 90,
                    'memory_usage': 90
                },
                'channels': {
                    'email': {
                        'smtp_host': 'localhost',
                        'smtp_port': 25,
                        'from': 'test@example.com',
                        'to': 'admin@example.com'
                    }
                }
            }
        },
        'backup': {
            'strategies': {
                'daily': {
                    'schedule': '0 0 * * *',
                    'retention': 7,
                    'destination': 'local',
                    'compress': True
                }
            },
            'scheduler': {
                'max_retries': 3,
                'retry_delay': 300
            }
        },
        'tiering': {
            'policies': {
                'hot': {
                    'storage': 'local',
                    'max_age': '7d',
                    'min_access': 10,
                    'priority': 100
                },
                'warm': {
                    'storage': 's3',
                    'max_age': '30d',
                    'min_access': 5,
                    'priority': 50
                }
            }
        }
    }

@pytest.fixture
async def storage_backends(
    test_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Create test storage backends."""
    backends = {}
    
    # Set up local storage
    local_path = Path(
        test_config['storage']['local']['base_path']
    )
    local_path.mkdir(parents=True, exist_ok=True)
    backends['local'] = LocalStorage(
        test_config['storage']['local']
    )
    
    # Set up S3 storage (using MinIO for testing)
    backends['s3'] = S3Storage(
        test_config['storage']['s3']
    )
    
    yield backends
    
    # Cleanup
    try:
        shutil.rmtree(local_path)
    except Exception:
        pass
    
    for backend in backends.values():
        backend.close()

@pytest.fixture
async def monitoring_system(
    test_config: Dict[str, Any],
    storage_backends: Dict[str, Any]
) -> MonitoringSystem:
    """Create test monitoring system."""
    system = MonitoringSystem(
        test_config['monitoring'],
        storage_backends
    )
    yield system
    system.close()

@pytest.fixture
async def backup_manager(
    test_config: Dict[str, Any],
    storage_backends: Dict[str, Any]
) -> BackupManager:
    """Create test backup manager."""
    manager = BackupManager(
        test_config['backup'],
        storage_backends
    )
    yield manager
    manager.stop()

@pytest.fixture
async def tiering_manager(
    test_config: Dict[str, Any],
    storage_backends: Dict[str, Any]
) -> TieringManager:
    """Create test tiering manager."""
    manager = TieringManager(
        test_config['tiering'],
        storage_backends
    )
    yield manager
    manager.close()

@pytest.fixture
def test_data() -> bytes:
    """Create test data."""
    return b"Test data content"

@pytest.fixture
def test_file(tmp_path: Path) -> Path:
    """Create test file."""
    file_path = tmp_path / "test.txt"
    file_path.write_bytes(b"Test file content")
    return file_path 