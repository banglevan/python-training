"""
Test API endpoints.
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
from src.dashboard.backend.api import app
from src.utils.config import Config

client = TestClient(app)

@pytest.fixture
def config():
    """Test configuration."""
    return Config("tests/fixtures/config")

def test_get_sales_metrics():
    """Test sales metrics endpoint."""
    response = client.get("/api/metrics/sales")
    assert response.status_code == 200
    
    data = response.json()
    assert "metrics" in data
    assert "trends" in data

def test_get_sales_metrics_with_dates():
    """Test sales metrics with date filters."""
    params = {
        'start_date': '2024-01-01',
        'end_date': '2024-01-31'
    }
    response = client.get("/api/metrics/sales", params=params)
    assert response.status_code == 200

def test_get_customer_metrics():
    """Test customer metrics endpoint."""
    response = client.get("/api/metrics/customers")
    assert response.status_code == 200
    
    data = response.json()
    assert "metrics" in data
    assert "trends" in data

def test_get_product_metrics():
    """Test product metrics endpoint."""
    response = client.get("/api/metrics/products")
    assert response.status_code == 200
    
    data = response.json()
    assert "metrics" in data
    assert "top_products" in data

def test_get_product_metrics_with_category():
    """Test product metrics with category filter."""
    params = {'category': 'Electronics'}
    response = client.get("/api/metrics/products", params=params)
    assert response.status_code == 200 