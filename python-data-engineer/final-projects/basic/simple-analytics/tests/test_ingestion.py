"""
Test ingestion components.
"""

import pytest
import pandas as pd
from datetime import datetime
from src.utils.config import Config
from src.ingestion.connectors.csv import CSVConnector
from src.ingestion.connectors.database import DatabaseConnector
from src.ingestion.validators.schema import SchemaValidator
from src.ingestion.loaders.warehouse import WarehouseLoader

@pytest.fixture
def config():
    """Test configuration."""
    return Config("tests/fixtures/config")

@pytest.fixture
def sample_data():
    """Sample test data."""
    return pd.DataFrame({
        'sale_id': range(1, 6),
        'date': [datetime.now()] * 5,
        'customer_id': range(101, 106),
        'product_id': range(201, 206),
        'quantity': [1, 2, 3, 4, 5],
        'unit_price': [10.0] * 5,
        'total_amount': [10.0, 20.0, 30.0, 40.0, 50.0]
    })

def test_csv_connector(config):
    """Test CSV connector."""
    connector = CSVConnector(config)
    connector.connect()
    
    df = connector.read()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    
    connector.close()

def test_database_connector(config):
    """Test database connector."""
    connector = DatabaseConnector(config)
    connector.connect()
    
    df = connector.read("SELECT * FROM test_table LIMIT 5")
    assert isinstance(df, pd.DataFrame)
    assert len(df) <= 5
    
    connector.close()

def test_schema_validator(config, sample_data):
    """Test schema validation."""
    validator = SchemaValidator(config)
    
    # Test valid data
    assert validator.validate(sample_data, 'sales')
    assert len(validator.get_errors()) == 0
    
    # Test invalid data
    invalid_data = sample_data.copy()
    invalid_data['quantity'] = ['a', 'b', 'c', 'd', 'e']
    assert not validator.validate(invalid_data, 'sales')
    assert len(validator.get_errors()) > 0

def test_warehouse_loader(config, sample_data):
    """Test warehouse loader."""
    loader = WarehouseLoader(config)
    
    # Test dimension load
    loader.load_dimension(
        sample_data,
        'dim_customers',
        ['customer_id']
    )
    
    # Test fact load
    loader.load_fact(
        sample_data,
        'fact_sales',
        ['sale_id']
    ) 