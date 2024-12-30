"""
Unit tests for feature transformers.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from src.features.transformations.customer_transforms import CustomerFeatureTransformer
from src.features.transformations.product_transforms import ProductFeatureTransformer

@pytest.fixture
def sample_orders():
    """Sample order data for testing."""
    return pd.DataFrame({
        'customer_id': [1, 1, 2],
        'order_id': ['A1', 'A2', 'B1'],
        'order_amount': [100.0, 200.0, 150.0],
        'order_date': [
            datetime.now() - timedelta(days=5),
            datetime.now() - timedelta(days=2),
            datetime.now() - timedelta(days=1)
        ],
        'product_id': ['P1', 'P2', 'P1'],
        'category': ['Electronics', 'Books', 'Electronics']
    })

def test_customer_order_features():
    """Test customer order feature computation."""
    transformer = CustomerFeatureTransformer()
    orders = sample_orders()
    
    features = transformer.compute_order_features(orders, customer_id=1)
    
    assert features['total_orders'] == 2
    assert features['total_amount'] == 300.0
    assert features['avg_order_value'] == 150.0
    assert features['days_since_last_order'] <= 2

def test_product_sales_features():
    """Test product sales feature computation."""
    transformer = ProductFeatureTransformer()
    orders = sample_orders()
    
    features = transformer.compute_sales_features(orders, product_id='P1')
    
    assert features['total_sales'] == 2
    assert 'sales_velocity' in features 