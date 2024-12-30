"""
Test analytics components.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.utils.config import Config
from src.analytics.processors.metrics import MetricsProcessor
from src.analytics.processors.trends import TrendDetector
from src.analytics.calculators.metrics import MetricsCalculator
from src.analytics.aggregators.time import TimeAggregator
from src.analytics.aggregators.dimension import DimensionAggregator

@pytest.fixture
def config():
    """Test configuration."""
    return Config("tests/fixtures/config")

@pytest.fixture
def sample_data():
    """Sample time series data."""
    dates = pd.date_range(
        start='2024-01-01',
        end='2024-01-31',
        freq='D'
    )
    return pd.DataFrame({
        'date': dates,
        'value': np.random.normal(100, 10, len(dates)),
        'category': ['A', 'B'] * (len(dates) // 2 + 1)
    })

def test_metrics_processor(config, sample_data):
    """Test metrics processing."""
    processor = MetricsProcessor(config)
    
    metrics = processor.process_sales_metrics(
        sample_data,
        'date'
    )
    assert isinstance(metrics, pd.DataFrame)
    assert not metrics.empty

def test_trend_detector(config, sample_data):
    """Test trend detection."""
    detector = TrendDetector(config)
    
    trends = detector.detect_trends(
        sample_data,
        'value',
        'date'
    )
    assert isinstance(trends, dict)
    assert 'direction' in trends
    assert 'strength' in trends

def test_metrics_calculator(config):
    """Test metrics calculation."""
    calculator = MetricsCalculator(config)
    
    # Test growth
    assert calculator.calculate_growth(110, 100) == 0.1
    
    # Test moving average
    series = pd.Series([1, 2, 3, 4, 5])
    ma = calculator.calculate_moving_average(series, window=3)
    assert len(ma) == len(series)

def test_time_aggregator(config, sample_data):
    """Test time aggregation."""
    aggregator = TimeAggregator(config)
    
    daily = aggregator.aggregate_daily(
        sample_data,
        'date',
        [{'name': 'value', 'type': 'sum'}]
    )
    assert isinstance(daily, pd.DataFrame)
    assert len(daily) == len(sample_data['date'].unique())

def test_dimension_aggregator(config, sample_data):
    """Test dimension aggregation."""
    aggregator = DimensionAggregator(config)
    
    result = aggregator.aggregate_by_dimension(
        sample_data,
        'category',
        [{'name': 'value', 'type': 'mean'}]
    )
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(sample_data['category'].unique()) 