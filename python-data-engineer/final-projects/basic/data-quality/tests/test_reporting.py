"""
Test reporting components.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from src.reporting.metrics import QualityMetrics
from src.reporting.dashboard import QualityDashboard
from src.utils.config import Config

@pytest.fixture
def config():
    """Test configuration."""
    return Config("tests/fixtures/config")

@pytest.fixture
def test_check_results():
    """Test quality check results."""
    return [
        {
            'type': 'completeness',
            'column': 'email',
            'score': 0.95,
            'threshold': 0.9,
            'status': 'passed'
        },
        {
            'type': 'uniqueness',
            'column': 'email',
            'score': 1.0,
            'threshold': 1.0,
            'status': 'passed'
        },
        {
            'type': 'format',
            'column': 'email',
            'score': 0.98,
            'pattern': '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$',
            'status': 'passed'
        }
    ]

@pytest.fixture
def test_metrics_history():
    """Test metrics history."""
    dates = pd.date_range(
        end=datetime.now(),
        periods=10,
        freq='D'
    )
    
    return [
        {
            'timestamp': date.isoformat(),
            'quality_score': np.random.uniform(0.8, 1.0),
            'completeness_score': np.random.uniform(0.9, 1.0),
            'uniqueness_score': np.random.uniform(0.9, 1.0),
            'format_score': np.random.uniform(0.9, 1.0)
        }
        for date in dates
    ]

def test_metrics_calculation(config, test_check_results):
    """Test metrics calculation."""
    calculator = QualityMetrics(config)
    metrics = calculator.calculate_dataset_metrics(test_check_results)
    
    assert 'quality_score' in metrics
    assert metrics['quality_score'] > 0
    assert metrics['quality_score'] <= 1
    
    assert 'completeness_score' in metrics
    assert 'uniqueness_score' in metrics
    assert 'format_score' in metrics

def test_trend_calculation(config, test_metrics_history):
    """Test trend calculation."""
    calculator = QualityMetrics(config)
    trends = calculator.calculate_trend_metrics(test_metrics_history)
    
    assert 'quality_score' in trends
    assert 'trend' in trends['quality_score']
    assert 'change' in trends['quality_score']

def test_dashboard_plots(config, test_check_results, test_metrics_history, tmp_path):
    """Test dashboard plot generation."""
    dashboard = QualityDashboard(config)
    
    # Test trend plot
    trend_plot = tmp_path / "trend.png"
    dashboard.create_quality_trend_plot(
        test_metrics_history,
        trend_plot
    )
    assert trend_plot.exists()
    
    # Test issues plot
    issues_plot = tmp_path / "issues.png"
    dashboard.create_issues_summary_plot(
        test_check_results,
        issues_plot
    )
    assert issues_plot.exists()

def test_html_report(config, test_check_results, test_metrics_history, tmp_path):
    """Test HTML report generation."""
    dashboard = QualityDashboard(config)
    
    # Calculate metrics
    calculator = QualityMetrics(config)
    metrics = calculator.calculate_dataset_metrics(test_check_results)
    trends = calculator.calculate_trend_metrics(test_metrics_history)
    
    # Generate report
    report_file = tmp_path / "report.html"
    dashboard.generate_html_report(
        metrics,
        trends,
        test_check_results,
        report_file
    )
    
    assert report_file.exists()
    with open(report_file, 'r') as f:
        content = f.read()
        assert 'Data Quality Report' in content
        assert str(round(metrics['quality_score'] * 100, 2)) in content 