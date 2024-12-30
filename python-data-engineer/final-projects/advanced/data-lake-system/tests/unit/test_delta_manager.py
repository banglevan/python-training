"""
Unit tests for Delta manager.
"""

import pytest
from pyspark.sql import SparkSession
from src.storage.delta.delta_manager import DeltaManager

@pytest.fixture
def spark():
    """Create test Spark session."""
    return (
        SparkSession.builder
        .appName("test")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

@pytest.fixture
def delta_manager(spark):
    """Create test Delta manager."""
    return DeltaManager(spark)

def test_write_table(spark, delta_manager, tmp_path):
    """Test writing Delta table."""
    # Create test data
    data = [
        (1, "test1"),
        (2, "test2")
    ]
    df = spark.createDataFrame(
        data,
        ["id", "name"]
    )
    
    # Write table
    table_path = str(tmp_path / "test_table")
    delta_manager.write_table(
        df,
        table_path,
        mode="overwrite"
    )
    
    # Verify table
    result = spark.read.format("delta").load(table_path)
    assert result.count() == 2
    assert set(result.columns) == {"id", "name"}

def test_merge_table(spark, delta_manager, tmp_path):
    """Test merging Delta table."""
    # Create base data
    base_data = [
        (1, "test1"),
        (2, "test2")
    ]
    base_df = spark.createDataFrame(
        base_data,
        ["id", "name"]
    )
    
    # Create update data
    update_data = [
        (2, "test2_updated"),
        (3, "test3")
    ]
    update_df = spark.createDataFrame(
        update_data,
        ["id", "name"]
    )
    
    # Write base table
    table_path = str(tmp_path / "test_merge")
    delta_manager.write_table(
        base_df,
        table_path,
        mode="overwrite"
    )
    
    # Merge updates
    delta_manager.merge_table(
        update_df,
        table_path,
        "t.id = s.id",
        {"name": "s.name"},
        {"name": "s.name"}
    )
    
    # Verify results
    result = spark.read.format("delta").load(table_path)
    assert result.count() == 3
    assert result.filter("id = 2").first()["name"] == "test2_updated" 