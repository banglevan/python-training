"""
Data lineage tracking.
"""

from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from ..metadata.atlas_manager import AtlasManager

logger = logging.getLogger(__name__)

class LineageTracker:
    """Track data lineage."""
    
    def __init__(self, atlas_manager: AtlasManager):
        """
        Initialize lineage tracker.
        
        Args:
            atlas_manager: Atlas manager instance
        """
        self.atlas = atlas_manager
    
    def track_transformation(
        self,
        source_table: str,
        target_table: str,
        transformation_type: str,
        transformation_details: Dict[str, Any]
    ) -> None:
        """
        Track data transformation.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            transformation_type: Type of transformation
            transformation_details: Transformation details
        """
        try:
            logger.info(
                f"Tracking transformation: {source_table} -> "
                f"{target_table}"
            )
            
            # Get entity GUIDs
            source_guid = self._get_entity_guid(source_table)
            target_guid = self._get_entity_guid(target_table)
            
            # Create process entity
            process_name = (
                f"transform_{transformation_type}_"
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            
            self.atlas.update_lineage(
                source_guid,
                target_guid,
                process_name,
                transformation_type
            )
            
            logger.info("Transformation tracked successfully")
            
        except Exception as e:
            logger.error(f"Failed to track transformation: {e}")
            raise
    
    def track_data_quality(
        self,
        table_name: str,
        quality_checks: List[Dict[str, Any]]
    ) -> None:
        """
        Track data quality checks.
        
        Args:
            table_name: Table name
            quality_checks: Quality check results
        """
        try:
            logger.info(f"Tracking quality checks for {table_name}")
            
            entity_guid = self._get_entity_guid(table_name)
            
            # Add quality classification
            self.atlas.add_classification(
                entity_guid,
                "data_quality",
                {
                    "checkTimestamp": datetime.now().isoformat(),
                    "qualityScore": self._calculate_quality_score(
                        quality_checks
                    ),
                    "checkResults": json.dumps(quality_checks)
                }
            )
            
            logger.info("Quality checks tracked successfully")
            
        except Exception as e:
            logger.error(f"Failed to track quality: {e}")
            raise
    
    def _get_entity_guid(self, table_name: str) -> str:
        """Get entity GUID by table name."""
        try:
            response = self.atlas.client.entity.get_by_attribute(
                "delta_table",
                [("qualifiedName", f"delta_lake.{table_name}")]
            )
            
            if not response.entities:
                raise ValueError(
                    f"Entity not found for table {table_name}"
                )
            
            return response.entities[0].guid
            
        except Exception as e:
            logger.error(f"Failed to get entity GUID: {e}")
            raise
    
    def _calculate_quality_score(
        self,
        quality_checks: List[Dict[str, Any]]
    ) -> float:
        """Calculate overall quality score."""
        if not quality_checks:
            return 0.0
            
        passed_checks = sum(
            1 for check in quality_checks
            if check.get('passed', False)
        )
        return passed_checks / len(quality_checks) 