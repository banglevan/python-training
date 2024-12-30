"""
Test validation components.
"""

import pytest
import pandas as pd
from src.validation.schema import SchemaValidator
from src.validation.checks import QualityChecker
from src.utils.config import Config

@pytest.fixture
def config():
    """Test configuration."""
    return Config("tests/fixtures/config")

@pytest.fixture
def test_data():
    """Test dataset."""
    return pd.DataFrame({
        'id': range(1, 11),
        'email': [f'user{i}@test.com' for i in range(1, 11)],
        'age': [20 + i for i in range(10)],
        'created_at': pd.date_range('2024-01-01', periods=10)
    })

def test_schema_validation(config, test_data):
    """Test schema validation."""
    validator = SchemaValidator(config)
    assert validator.validate_schema(test_data, 'test_dataset')
    assert len(validator.get_validation_errors()) == 0

def test_completeness_check(config, test_data):
    """Test completeness check."""
    checker = QualityChecker(config)
    assert checker.check_completeness(
        test_data,
        ['email', 'age'],
        0.9
    )

def test_uniqueness_check(config, test_data):
    """Test uniqueness check."""
    checker = QualityChecker(config)
    assert checker.check_uniqueness(
        test_data,
        ['email'],
        1.0
    )

def test_format_check(config, test_data):
    """Test format check."""
    checker = QualityChecker(config)
    assert checker.check_format(
        test_data,
        'email',
        '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
    ) 