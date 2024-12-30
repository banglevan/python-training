"""
Test alert components.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from src.alerts.notification import AlertNotifier
from src.alerts.threshold import ThresholdManager
from src.utils.config import Config

@pytest.fixture
def config():
    """Test configuration."""
    return Config("tests/fixtures/config")

@pytest.fixture
def test_alert_details():
    """Test alert details."""
    return {
        'dataset': 'customers',
        'metric': 'quality_score',
        'value': 0.75,
        'threshold': 0.8,
        'check_results': [
            {
                'type': 'completeness',
                'column': 'email',
                'score': 0.75,
                'threshold': 0.9,
                'status': 'failed'
            }
        ]
    }

def test_threshold_check(config):
    """Test threshold checking."""
    manager = ThresholdManager(config)
    
    # Test critical threshold
    assert manager.check_threshold('quality_score', 0.75) == 'critical'
    
    # Test warning threshold
    assert manager.check_threshold('quality_score', 0.85) == 'warning'
    
    # Test info threshold
    assert manager.check_threshold('quality_score', 0.92) == 'info'
    
    # Test no threshold breach
    assert manager.check_threshold('quality_score', 0.98) is None

@patch('smtplib.SMTP')
def test_alert_notification(mock_smtp, config, test_alert_details):
    """Test alert notification."""
    notifier = AlertNotifier(config)
    
    # Mock SMTP connection
    mock_smtp_instance = MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_smtp_instance
    
    # Send alert
    notifier.send_alert(
        'critical',
        'Quality score below threshold',
        test_alert_details
    )
    
    # Verify SMTP calls
    assert mock_smtp.called
    assert mock_smtp_instance.send_message.called
    
    # Verify email content
    call_args = mock_smtp_instance.send_message.call_args[0][0]
    assert 'CRITICAL' in call_args['Subject']
    assert 'Quality score below threshold' in str(call_args.get_payload()[0])

@patch('smtplib.SMTP')
def test_alert_digest(mock_smtp, config):
    """Test alert digest."""
    notifier = AlertNotifier(config)
    
    # Create test alerts
    alerts = [
        {
            'level': 'critical',
            'message': 'Quality score below threshold',
            'timestamp': datetime.now().isoformat(),
            'details': {
                'dataset': 'customers',
                'metric': 'quality_score',
                'value': 0.75
            }
        },
        {
            'level': 'warning',
            'message': 'Completeness check failed',
            'timestamp': datetime.now().isoformat(),
            'details': {
                'dataset': 'orders',
                'metric': 'completeness_score',
                'value': 0.85
            }
        }
    ]
    
    # Mock SMTP connection
    mock_smtp_instance = MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_smtp_instance
    
    # Send digest
    notifier.send_digest(alerts)
    
    # Verify SMTP calls
    assert mock_smtp.called
    assert mock_smtp_instance.send_message.called
    
    # Verify email content
    call_args = mock_smtp_instance.send_message.call_args[0][0]
    assert 'Alert Digest' in call_args['Subject']
    digest_content = str(call_args.get_payload()[0])
    assert 'Total Alerts: 2' in digest_content
    assert 'Quality score below threshold' in digest_content
    assert 'Completeness check failed' in digest_content

def test_dynamic_thresholds(config):
    """Test dynamic threshold calculation."""
    manager = ThresholdManager(config)
    
    # Test threshold calculation
    metric = 'quality_score'
    history = [0.95, 0.92, 0.88, 0.90, 0.93]
    
    with patch.object(
        manager,
        '_load_metric_history',
        return_value=history
    ):
        manager._calculate_dynamic_thresholds()
        
        # Verify thresholds
        mean = sum(history) / len(history)
        std = (sum((x - mean) ** 2 for x in history) / len(history)) ** 0.5
        
        assert manager.get_threshold(metric, 'critical') == pytest.approx(mean - 3 * std)
        assert manager.get_threshold(metric, 'warning') == pytest.approx(mean - 2 * std)
        assert manager.get_threshold(metric, 'info') == pytest.approx(mean - std) 