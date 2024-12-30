"""
Monitoring Tests
-------------

Test cases for monitoring system:
1. Metrics collection
2. Alert generation
3. Health checks
4. System status
"""

import pytest
from datetime import datetime, timedelta
import asyncio
from typing import Dict, Any
import json
from pathlib import Path

@pytest.mark.asyncio
async def test_metrics_collection(
    monitoring_system,
    storage_backends: Dict[str, Any],
    test_file: Path
):
    """Test system metrics collection."""
    # Store test file to generate metrics
    for storage in storage_backends.values():
        await asyncio.to_thread(
            storage.store,
            test_file,
            {'purpose': 'testing'}
        )
    
    # Collect metrics
    metrics = await monitoring_system.metrics.collect_metrics(
        storage_backends
    )
    
    # Verify basic metric structure
    assert 'timestamp' in metrics
    assert 'system' in metrics
    assert 'storage' in metrics
    
    # Check system metrics
    system = metrics['system']
    assert 'cpu' in system
    assert 'memory' in system
    assert 'disk' in system
    assert 'network' in system
    
    # Check storage metrics
    storage = metrics['storage']
    for backend_name in storage_backends:
        assert backend_name in storage
        backend_metrics = storage[backend_name]
        assert 'total_space' in backend_metrics
        assert 'used_space' in backend_metrics
        assert 'file_count' in backend_metrics

@pytest.mark.asyncio
async def test_alert_generation(
    monitoring_system,
    storage_backends: Dict[str, Any]
):
    """Test alert generation and management."""
    # Create test metrics that should trigger alerts
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'storage': {
            'local': {
                'used_percent': 90,  # Should trigger alert
                'total_space': 1000,
                'used_space': 900
            }
        },
        'system': {
            'cpu': {
                'percent': 95  # Should trigger alert
            },
            'memory': {
                'percent': 85
            }
        }
    }
    
    # Check for alerts
    alerts = await monitoring_system.alerts.check_alerts(
        metrics
    )
    
    # Verify alerts were generated
    assert len(alerts) >= 2  # Should have CPU and storage alerts
    
    # Check alert details
    alert_types = [a['type'] for a in alerts]
    assert 'storage_usage' in alert_types
    assert 'cpu_usage' in alert_types
    
    # Verify alert storage
    stored_alerts = monitoring_system.alerts.get_alerts(
        start_time=datetime.now() - timedelta(minutes=5)
    )
    assert len(stored_alerts) >= 2

@pytest.mark.asyncio
async def test_health_checks(
    monitoring_system,
    storage_backends: Dict[str, Any]
):
    """Test system health checks."""
    # Run health check
    health = await monitoring_system.check_health()
    
    # Verify health check structure
    assert 'timestamp' in health
    assert 'status' in health
    assert 'checks' in health
    
    # Check storage backend health
    checks = health['checks']
    for backend_name in storage_backends:
        assert backend_name in checks
        backend_health = checks[backend_name]
        assert 'status' in backend_health
        if backend_health['status'] == 'healthy':
            assert 'latency' in backend_health
    
    # Check system resource health
    assert 'cpu' in checks
    assert 'memory' in checks
    assert any(
        check.startswith('disk_')
        for check in checks
    )

@pytest.mark.asyncio
async def test_monitoring_loop(
    monitoring_system,
    storage_backends: Dict[str, Any]
):
    """Test monitoring system main loop."""
    # Start monitoring
    monitor_task = asyncio.create_task(
        monitoring_system.start_monitoring()
    )
    
    # Wait for a few monitoring cycles
    await asyncio.sleep(
        monitoring_system.interval * 2
    )
    
    # Get current status
    status = monitoring_system.get_system_status()
    
    # Stop monitoring
    monitoring_system.stop_monitoring()
    await monitor_task
    
    # Verify status
    assert 'timestamp' in status
    if 'metrics' in status:
        assert len(status['metrics']) > 0
    if 'alerts' in status:
        assert isinstance(status['alerts'], list)
    if 'health' in status:
        assert status['health']['status'] in {
            'healthy',
            'degraded',
            'unhealthy'
        }

@pytest.mark.asyncio
async def test_metrics_retention(
    monitoring_system,
    storage_backends: Dict[str, Any]
):
    """Test metrics retention and cleanup."""
    # Collect some metrics
    for _ in range(3):
        await monitoring_system.metrics.collect_metrics(
            storage_backends
        )
        await asyncio.sleep(1)
    
    # Get initial metrics count
    initial_metrics = monitoring_system.metrics.get_metrics()
    initial_count = len(initial_metrics)
    
    # Clean up old metrics
    await monitoring_system.metrics.cleanup_old_metrics()
    
    # Verify retention
    current_metrics = monitoring_system.metrics.get_metrics()
    assert len(current_metrics) <= initial_count

@pytest.mark.asyncio
async def test_alert_notifications(
    monitoring_system,
    tmp_path: Path
):
    """Test alert notification channels."""
    # Create test alert
    test_alert = {
        'type': 'test_alert',
        'severity': 'warning',
        'message': 'Test alert message',
        'timestamp': datetime.now().isoformat()
    }
    
    # Test email notifications
    if 'email' in monitoring_system.alerts.channels:
        # Redirect email output to file for testing
        test_email = tmp_path / "test_email.txt"
        monitoring_system.alerts.config['email'].update({
            'smtp_host': 'localhost',
            'smtp_port': 25,
            'output_file': str(test_email)
        })
        
        await monitoring_system.alerts._send_notifications(
            [test_alert]
        )
        
        # Verify email was "sent"
        assert test_email.exists()
        content = test_email.read_text()
        assert 'Test alert message' in content

@pytest.mark.asyncio
async def test_system_status_storage(
    monitoring_system,
    storage_backends: Dict[str, Any]
):
    """Test system status storage and retrieval."""
    # Generate test status data
    metrics = await monitoring_system.metrics.collect_metrics(
        storage_backends
    )
    alerts = await monitoring_system.alerts.check_alerts(
        metrics
    )
    health = await monitoring_system.check_health()
    
    # Store status
    await monitoring_system._store_status(
        metrics,
        alerts,
        health
    )
    
    # Get current status
    status = monitoring_system.get_system_status()
    
    # Verify status data
    assert isinstance(status, dict)
    assert 'timestamp' in status
    if 'metrics' in status:
        assert isinstance(status['metrics'], list)
    if 'alerts' in status:
        assert isinstance(status['alerts'], list)
    if 'health' in status:
        assert isinstance(status['health'], dict) 