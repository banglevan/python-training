"""
Main Application Tests
-----------------

Test cases for pipeline application.
"""

import pytest
from unittest.mock import Mock, patch
import signal
from datetime import datetime

from pipeline.models import SensorData
from main import PipelineApp

@pytest.fixture
def mock_collector():
    """Create mock collector."""
    with patch('pipeline.collectors.MQTTCollector') as mock:
        yield mock.return_value

@pytest.fixture
def mock_processor():
    """Create mock processor."""
    with patch('pipeline.processors.DataProcessor') as mock:
        yield mock.return_value

@pytest.fixture
def mock_storage():
    """Create mock storage."""
    with patch('pipeline.storage.DataStorage') as mock:
        yield mock.return_value

@pytest.fixture
def app(mock_collector, mock_processor, mock_storage):
    """Create application instance."""
    app = PipelineApp()
    yield app
    app.stop()

def test_app_start(app, mock_collector, mock_processor):
    """Test application start."""
    app.start()
    
    # Verify component startup
    mock_collector.start.assert_called_once()
    assert app.processor_thread is not None
    assert app.running

def test_app_stop(app, mock_collector, mock_processor, mock_storage):
    """Test application stop."""
    app.start()
    app.stop()
    
    # Verify component shutdown
    mock_collector.stop.assert_called_once()
    mock_processor.stop.assert_called_once()
    mock_storage.close.assert_called_once()
    assert not app.running

def test_sensor_data_handling(app, mock_storage):
    """Test sensor data handling."""
    # Test data
    data = SensorData(
        sensor_id='test',
        metric='temperature',
        value=25.5,
        timestamp=datetime.now(),
        metadata={'unit': 'C'}
    )
    
    # Handle data
    app._handle_sensor_data(data)
    
    # Verify storage operations
    assert mock_storage.store_measurement.called
    assert mock_storage.cache_data.called

def test_signal_handling(app):
    """Test signal handling."""
    app.start()
    
    # Simulate SIGTERM
    app._signal_handler(signal.SIGTERM, None)
    
    assert not app.running

def test_error_handling(app, mock_collector):
    """Test error handling."""
    # Simulate startup error
    mock_collector.start.side_effect = Exception("Start failed")
    
    with pytest.raises(Exception):
        app.start()
    
    assert not app.running 