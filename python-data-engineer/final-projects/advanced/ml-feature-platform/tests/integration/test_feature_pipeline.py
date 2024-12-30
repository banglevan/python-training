"""
Integration tests for feature pipeline.
"""

import pytest
from datetime import datetime, timedelta
from src.data.sources.postgresql import PostgreSQLSource
from src.features.transformations.customer_transforms import CustomerFeatureTransformer
from src.data.sinks.feast import FeastSink
from src.features.validation.validators import FeatureValidator

@pytest.fixture
def setup_pipeline(postgresql_db):
    """Setup test pipeline components."""
    source = PostgreSQLSource(postgresql_db.connection_string)
    transformer = CustomerFeatureTransformer()
    sink = FeastSink("test_repo/")
    validator = FeatureValidator()
    
    return source, transformer, sink, validator

def test_end_to_end_pipeline(setup_pipeline):
    """Test complete feature pipeline."""
    source, transformer, sink, validator = setup_pipeline
    
    # Extract data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    data = source.extract_customer_features(start_date, end_date)
    
    # Transform features
    customer_id = data['customer_id'].iloc[0]
    features = transformer.compute_order_features(data, customer_id)
    
    # Validate features
    validation_result = validator.validate_features(
        pd.DataFrame([features])
    )
    assert validation_result['passed']
    
    # Store features
    sink.push_features_to_online_store(
        "customer_features",
        pd.DataFrame([features])
    )
    
    # Verify storage
    stored_features = sink.get_online_features(
        feature_refs=["customer_features:total_orders"],
        entity_rows=[{"customer_id": customer_id}]
    )
    assert len(stored_features) > 0 