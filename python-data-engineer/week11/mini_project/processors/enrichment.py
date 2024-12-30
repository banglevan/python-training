"""
Data enrichment components.
"""

from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

logger = logging.getLogger(__name__)

class DataEnrichment:
    """Data enrichment operations."""
    
    def __init__(self, spark: SparkSession):
        """Initialize enrichment processor."""
        self.spark = spark
    
    def add_technical_metadata(
        self,
        data: DataFrame,
        content_col: str = "content"
    ) -> DataFrame:
        """
        Add technical metadata.
        
        Args:
            data: Input DataFrame
            content_col: Column containing content
        
        Returns:
            Enriched DataFrame
        """
        try:
            # Add hash
            data = data.withColumn(
                "content_hash",
                sha2(col(content_col), 256)
            )
            
            # Add size
            data = data.withColumn(
                "file_size_bytes",
                length(col(content_col))
            )
            
            # Add processing timestamp
            data = data.withColumn(
                "processed_at",
                current_timestamp()
            )
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to add technical metadata: {e}")
            raise
    
    def add_business_metadata(
        self,
        data: DataFrame,
        business_rules: Dict[str, Any]
    ) -> DataFrame:
        """
        Add business metadata.
        
        Args:
            data: Input DataFrame
            business_rules: Business rules to apply
        
        Returns:
            Enriched DataFrame
        """
        try:
            # Add classification
            if 'size_rules' in business_rules:
                data = data.withColumn(
                    "size_category",
                    when(col("file_size_bytes") < business_rules['size_rules']['small'], "small")
                    .when(col("file_size_bytes") < business_rules['size_rules']['medium'], "medium")
                    .otherwise("large")
                )
            
            # Add retention policy
            if 'retention_rules' in business_rules:
                data = data.withColumn(
                    "retention_days",
                    when(col("size_category") == "small", business_rules['retention_rules']['small'])
                    .when(col("size_category") == "medium", business_rules['retention_rules']['medium'])
                    .otherwise(business_rules['retention_rules']['large'])
                )
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to add business metadata: {e}")
            raise
    
    def join_reference_data(
        self,
        data: DataFrame,
        reference_data: DataFrame,
        join_cols: List[str],
        select_cols: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Join with reference data.
        
        Args:
            data: Input DataFrame
            reference_data: Reference DataFrame
            join_cols: Columns to join on
            select_cols: Columns to select from reference
        
        Returns:
            Enriched DataFrame
        """
        try:
            if select_cols:
                reference_data = reference_data.select(
                    join_cols + select_cols
                )
            
            return data.join(
                reference_data,
                join_cols,
                "left"
            )
            
        except Exception as e:
            logger.error(f"Failed to join reference data: {e}")
            raise
    
    def add_quality_scores(
        self,
        data: DataFrame,
        rules: Dict[str, float]
    ) -> DataFrame:
        """
        Add data quality scores.
        
        Args:
            data: Input DataFrame
            rules: Scoring rules
        
        Returns:
            Scored DataFrame
        """
        try:
            # Initialize score
            data = data.withColumn("quality_score", lit(100.0))
            
            # Apply deductions based on rules
            for column, deduction in rules.items():
                data = data.withColumn(
                    "quality_score",
                    when(
                        col(column).isNull(),
                        col("quality_score") - lit(deduction)
                    ).otherwise(col("quality_score"))
                )
            
            # Ensure score is between 0 and 100
            data = data.withColumn(
                "quality_score",
                greatest(least(col("quality_score"), lit(100.0)), lit(0.0))
            )
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to add quality scores: {e}")
            raise 