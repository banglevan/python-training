"""
Apache Hudi features exercise.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from typing import List, Dict, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HudiFeatures:
    """Apache Hudi advanced features."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """Initialize Hudi features."""
        self.spark = spark or SparkSession.builder \
            .appName("HudiFeatures") \
            .config("spark.jars.packages", 
                   "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.13.1") \
            .config("spark.sql.extensions",
                   "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
            .getOrCreate()
        
        logger.info("Hudi session initialized")
    
    def schedule_clustering(
        self,
        table_path: str,
        cluster_columns: List[str]
    ) -> None:
        """
        Schedule clustering operation.
        
        Args:
            table_path: Path to table
            cluster_columns: Columns to cluster by
        """
        try:
            self.spark.sql(f"""
                CALL spark_catalog.run_clustering(
                    table => '{table_path}',
                    order_by => '{",".join(cluster_columns)}'
                )
            """)
            
            logger.info(f"Scheduled clustering for {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to schedule clustering: {e}")
            raise
    
    def schedule_compaction(
        self,
        table_path: str,
        instant_time: Optional[str] = None
    ) -> None:
        """
        Schedule compaction operation.
        
        Args:
            table_path: Path to table
            instant_time: Instant time for compaction
        """
        try:
            if instant_time:
                self.spark.sql(f"""
                    CALL spark_catalog.schedule_compaction(
                        table => '{table_path}',
                        instant_time => '{instant_time}'
                    )
                """)
            else:
                self.spark.sql(f"""
                    CALL spark_catalog.schedule_compaction(
                        table => '{table_path}'
                    )
                """)
            
            logger.info(f"Scheduled compaction for {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to schedule compaction: {e}")
            raise
    
    def run_clean(
        self,
        table_path: str,
        retain_commits: int = 10,
        hours: int = 72
    ) -> None:
        """
        Run cleaning operation.
        
        Args:
            table_path: Path to table
            retain_commits: Number of commits to retain
            hours: Hours to look back
        """
        try:
            self.spark.sql(f"""
                CALL spark_catalog.run_clean(
                    table => '{table_path}',
                    retain_commits => {retain_commits},
                    hours => {hours}
                )
            """)
            
            logger.info(f"Ran cleaning for {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to run cleaning: {e}")
            raise
    
    def get_commit_metadata(
        self,
        table_path: str,
        limit: int = 10
    ) -> List[Dict]:
        """
        Get commit metadata.
        
        Args:
            table_path: Path to table
            limit: Maximum number of commits
        
        Returns:
            List of commit details
        """
        try:
            commits = self.spark.sql(f"""
                SELECT *
                FROM {table_path}.commits
                ORDER BY timestamp DESC
                LIMIT {limit}
            """).collect()
            
            return [row.asDict() for row in commits]
            
        except Exception as e:
            logger.error(f"Failed to get commit metadata: {e}")
            raise
    
    def set_table_config(
        self,
        table_path: str,
        config: Dict[str, str]
    ) -> None:
        """
        Set table configuration.
        
        Args:
            table_path: Path to table
            config: Configuration parameters
        """
        try:
            for key, value in config.items():
                self.spark.sql(f"""
                    CALL spark_catalog.set_table_config(
                        table => '{table_path}',
                        config_key => '{key}',
                        config_value => '{value}'
                    )
                """)
            
            logger.info(f"Updated config for {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to set table config: {e}")
            raise

if __name__ == "__main__":
    # Example usage
    hudi = HudiFeatures()
    
    try:
        table_path = "data/hudi/example"
        
        # Schedule clustering
        hudi.schedule_clustering(
            table_path,
            cluster_columns=["date", "id"]
        )
        
        # Schedule compaction
        hudi.schedule_compaction(table_path)
        
        # Run cleaning
        hudi.run_clean(
            table_path,
            retain_commits=5,
            hours=24
        )
        
        # Get commit metadata
        commits = hudi.get_commit_metadata(table_path)
        print("Commits:", commits)
        
        # Set table config
        hudi.set_table_config(
            table_path,
            {
                'hoodie.cleaner.commits.retained': '5',
                'hoodie.compact.inline.max.delta.commits': '5'
            }
        )
        
    finally:
        hudi.spark.stop() 