"""
Data governance components for Data Lake Platform.
"""

from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

class DataGovernance:
    """Data governance management."""
    
    def __init__(self, spark: SparkSession):
        """Initialize governance manager."""
        self.spark = spark
        self.audit_path = "audit/logs"
    
    def log_operation(
        self,
        operation: str,
        table_path: str,
        user: str,
        details: Dict[str, Any]
    ) -> None:
        """
        Log data operation.
        
        Args:
            operation: Operation type
            table_path: Affected table
            user: User performing operation
            details: Operation details
        """
        try:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'operation': operation,
                'table_path': table_path,
                'user': user,
                'details': details
            }
            
            # Create DataFrame from log
            log_df = self.spark.createDataFrame([log_entry])
            
            # Write to audit log
            log_df.write.format("delta") \
                .mode("append") \
                .save(self.audit_path)
            
            logger.info(f"Logged operation: {operation}")
            
        except Exception as e:
            logger.error(f"Failed to log operation: {e}")
            raise
    
    def enforce_schema(
        self,
        table_path: str,
        schema: StructType,
        evolution_mode: str = "additive"
    ) -> None:
        """
        Enforce schema constraints.
        
        Args:
            table_path: Path to table
            schema: Expected schema
            evolution_mode: Schema evolution mode
        """
        try:
            table = DeltaTable.forPath(self.spark, table_path)
            
            # Set schema enforcement
            table.alterTable() \
                .setProperty("delta.schemaEnforcement.enabled", "true") \
                .setProperty("delta.columnMapping.mode", evolution_mode) \
                .execute()
            
            logger.info(f"Enforced schema for: {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to enforce schema: {e}")
            raise
    
    def set_access_control(
        self,
        table_path: str,
        permissions: Dict[str, List[str]]
    ) -> None:
        """
        Set access control.
        
        Args:
            table_path: Path to table
            permissions: User/role permissions
        """
        try:
            for principal, actions in permissions.items():
                self.spark.sql(f"""
                    ALTER TABLE delta.`{table_path}`
                    SET PERMISSION {principal}
                    PRIVILEGES {','.join(actions)}
                """)
            
            logger.info(f"Set permissions for: {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to set permissions: {e}")
            raise
    
    def track_lineage(
        self,
        target_table: str,
        source_tables: List[str],
        transformation: str
    ) -> None:
        """
        Track data lineage.
        
        Args:
            target_table: Target table
            source_tables: Source tables
            transformation: Transformation details
        """
        try:
            lineage = {
                'timestamp': datetime.now().isoformat(),
                'target_table': target_table,
                'source_tables': source_tables,
                'transformation': transformation
            }
            
            # Create DataFrame from lineage
            lineage_df = self.spark.createDataFrame([lineage])
            
            # Write to lineage log
            lineage_df.write.format("delta") \
                .mode("append") \
                .save("audit/lineage")
            
            logger.info(f"Tracked lineage for: {target_table}")
            
        except Exception as e:
            logger.error(f"Failed to track lineage: {e}")
            raise
    
    def get_audit_history(
        self,
        table_path: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get audit history.
        
        Args:
            table_path: Filter by table
            start_time: Start timestamp
            end_time: End timestamp
        
        Returns:
            List of audit entries
        """
        try:
            query = self.spark.read.format("delta").load(self.audit_path)
            
            if table_path:
                query = query.filter(col("table_path") == table_path)
            
            if start_time:
                query = query.filter(col("timestamp") >= start_time)
            
            if end_time:
                query = query.filter(col("timestamp") <= end_time)
            
            return [row.asDict() for row in query.collect()]
            
        except Exception as e:
            logger.error(f"Failed to get audit history: {e}")
            raise 