"""
Apache Iceberg features exercise.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from typing import List, Dict, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IcebergFeatures:
    """Apache Iceberg advanced features."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """Initialize Iceberg features."""
        self.spark = spark or SparkSession.builder \
            .appName("IcebergFeatures") \
            .config("spark.jars.packages", 
                   "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1") \
            .config("spark.sql.extensions", 
                   "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", 
                   "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.local", 
                   "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", "warehouse") \
            .getOrCreate()
        
        logger.info("Iceberg session initialized")
    
    def manage_snapshots(
        self,
        table_name: str,
        action: str,
        snapshot_id: Optional[str] = None
    ) -> None:
        """
        Manage table snapshots.
        
        Args:
            table_name: Name of the table
            action: Action to perform (rollback/cherrypick)
            snapshot_id: ID of the snapshot
        """
        try:
            if action == "rollback" and snapshot_id:
                self.spark.sql(f"""
                    CALL local.system.rollback_to_snapshot(
                        table => '{table_name}',
                        snapshot_id => {snapshot_id}
                    )
                """)
                logger.info(f"Rolled back to snapshot {snapshot_id}")
                
            elif action == "cherrypick" and snapshot_id:
                self.spark.sql(f"""
                    CALL local.system.cherrypick_snapshot(
                        table => '{table_name}',
                        snapshot_id => {snapshot_id}
                    )
                """)
                logger.info(f"Cherry-picked snapshot {snapshot_id}")
                
            else:
                raise ValueError("Invalid action or missing snapshot_id")
            
        except Exception as e:
            logger.error(f"Failed to manage snapshot: {e}")
            raise
    
    def update_partition_spec(
        self,
        table_name: str,
        spec: Dict[str, str]
    ) -> None:
        """
        Update partition specification.
        
        Args:
            table_name: Name of the table
            spec: Partition specification
        """
        try:
            # Build partition transform expressions
            transforms = []
            for col, transform in spec.items():
                transforms.append(f"{transform}({col})")
            
            self.spark.sql(f"""
                ALTER TABLE {table_name}
                SET PARTITION SPEC (
                    {','.join(transforms)}
                )
            """)
            
            logger.info(f"Updated partition spec for {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to update partition spec: {e}")
            raise
    
    def maintain_table(
        self,
        table_name: str,
        actions: List[str]
    ) -> None:
        """
        Perform table maintenance.
        
        Args:
            table_name: Name of the table
            actions: List of maintenance actions
        """
        try:
            for action in actions:
                if action == "remove_orphans":
                    self.spark.sql(f"""
                        CALL local.system.remove_orphan_files(
                            table => '{table_name}'
                        )
                    """)
                    
                elif action == "compact":
                    self.spark.sql(f"""
                        CALL local.system.rewrite_data_files(
                            table => '{table_name}'
                        )
                    """)
                    
                elif action == "expire_snapshots":
                    self.spark.sql(f"""
                        CALL local.system.expire_snapshots(
                            table => '{table_name}',
                            retain_last => 1
                        )
                    """)
                    
                else:
                    logger.warning(f"Unknown maintenance action: {action}")
            
            logger.info(f"Completed maintenance for {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to perform maintenance: {e}")
            raise
    
    def get_snapshot_info(
        self,
        table_name: str,
        limit: int = 10
    ) -> List[Dict]:
        """
        Get snapshot information.
        
        Args:
            table_name: Name of the table
            limit: Maximum number of snapshots
        
        Returns:
            List of snapshot details
        """
        try:
            snapshots = self.spark.sql(f"""
                SELECT *
                FROM {table_name}.snapshots
                ORDER BY committed_at DESC
                LIMIT {limit}
            """).collect()
            
            return [row.asDict() for row in snapshots]
            
        except Exception as e:
            logger.error(f"Failed to get snapshot info: {e}")
            raise
    
    def set_table_properties(
        self,
        table_name: str,
        properties: Dict[str, str]
    ) -> None:
        """
        Set table properties.
        
        Args:
            table_name: Name of the table
            properties: Properties to set
        """
        try:
            props = [f"'{k}' = '{v}'" for k, v in properties.items()]
            
            self.spark.sql(f"""
                ALTER TABLE {table_name}
                SET TBLPROPERTIES (
                    {','.join(props)}
                )
            """)
            
            logger.info(f"Updated properties for {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to set properties: {e}")
            raise

if __name__ == "__main__":
    # Example usage
    iceberg = IcebergFeatures()
    
    try:
        table_name = "local.db.example"
        
        # Update partition spec
        iceberg.update_partition_spec(
            table_name,
            {
                "date": "day",
                "id": "bucket[16]"
            }
        )
        
        # Perform maintenance
        iceberg.maintain_table(
            table_name,
            ["remove_orphans", "compact", "expire_snapshots"]
        )
        
        # Get snapshot information
        snapshots = iceberg.get_snapshot_info(table_name)
        print("Snapshots:", snapshots)
        
        # Set properties
        iceberg.set_table_properties(
            table_name,
            {
                "write.metadata.compression-codec": "gzip",
                "write.metadata.metrics.default": "full"
            }
        )
        
    finally:
        iceberg.spark.stop() 