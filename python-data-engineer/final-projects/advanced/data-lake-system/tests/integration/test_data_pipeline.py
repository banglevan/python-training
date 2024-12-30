"""
Integration tests for data pipeline.
"""

import pytest
from datetime import datetime
from src.ingestion.batch.batch_ingestion import BatchIngestionJob
from src.storage.delta.delta_manager import DeltaManager
from src.catalog.metadata.atlas_manager import AtlasManager
from src.governance.policies.policy_manager import PolicyManager

@pytest.fixture
def setup_pipeline(spark, tmp_path):
    """Setup test pipeline components."""
    delta_manager = DeltaManager(spark)
    atlas_manager = AtlasManager(
        "localhost",
        21000,
        "admin",
        "admin"
    )
    policy_manager = PolicyManager(
        "config/policies.json",
        None
    )
    
    return delta_manager, atlas_manager, policy_manager

def test_end_to_end_pipeline(spark, setup_pipeline, tmp_path):
    """Test complete data pipeline."""
    delta_manager, atlas_manager, policy_manager = setup_pipeline
    
    # Create test data
    data = [
        (1, "test1", datetime.now()),
        (2, "test2", datetime.now())
    ]
    df = spark.createDataFrame(
        data,
        ["id", "name", "timestamp"]
    )
    
    # Write to Delta
    table_path = str(tmp_path / "test_pipeline")
    delta_manager.write_table(
        df,
        table_path,
        mode="overwrite"
    )
    
    # Register in Atlas
    guid = atlas_manager.register_table(
        "test_pipeline",
        {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "timestamp", "type": "timestamp"}
            ]
        }
    )
    
    # Verify access
    assert policy_manager.validate_access(
        "test_user",
        "read",
        table_path
    )
    
    # Read data
    result = spark.read.format("delta").load(table_path)
    assert result.count() == 2 