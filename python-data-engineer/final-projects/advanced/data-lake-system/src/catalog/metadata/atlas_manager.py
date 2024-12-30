"""
Apache Atlas metadata management.
"""

from typing import Dict, Any, List, Optional
import json
import logging
from datetime import datetime
from pyatlasclient import AtlasClient
from ...storage.delta.delta_manager import DeltaManager

logger = logging.getLogger(__name__)

class AtlasManager:
    """Manage Apache Atlas metadata."""
    
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str
    ):
        """
        Initialize Atlas manager.
        
        Args:
            host: Atlas host
            port: Atlas port
            username: Atlas username
            password: Atlas password
        """
        self.client = AtlasClient(
            host=host,
            port=port,
            username=username,
            password=password
        )
    
    def register_table(
        self,
        table_name: str,
        schema: Dict[str, Any],
        properties: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Register Delta table in Atlas.
        
        Args:
            table_name: Table name
            schema: Table schema
            properties: Additional properties
            
        Returns:
            Entity GUID
        """
        try:
            logger.info(f"Registering table {table_name}")
            
            # Create table entity
            entity = {
                "typeName": "delta_table",
                "attributes": {
                    "name": table_name,
                    "qualifiedName": f"delta_lake.{table_name}",
                    "description": properties.get(
                        'description',
                        f"Delta table {table_name}"
                    ),
                    "owner": properties.get('owner', 'data_lake'),
                    "createTime": datetime.now().isoformat(),
                    "schema": json.dumps(schema),
                    **properties or {}
                }
            }
            
            # Register entity
            response = self.client.entity.create(entity)
            guid = response.get('guidAssignments', {}).get(
                table_name
            )
            
            logger.info(f"Registered table with GUID: {guid}")
            return guid
            
        except Exception as e:
            logger.error(f"Failed to register table: {e}")
            raise
    
    def update_lineage(
        self,
        source_id: str,
        target_id: str,
        process_name: str,
        process_type: str
    ) -> str:
        """
        Update data lineage.
        
        Args:
            source_id: Source entity GUID
            target_id: Target entity GUID
            process_name: Process name
            process_type: Process type
            
        Returns:
            Process GUID
        """
        try:
            logger.info(
                f"Creating lineage: {source_id} -> {target_id}"
            )
            
            # Create process entity
            process = {
                "typeName": "Process",
                "attributes": {
                    "name": process_name,
                    "qualifiedName": (
                        f"process.{process_name}."
                        f"{datetime.now().isoformat()}"
                    ),
                    "processType": process_type,
                    "inputs": [{"guid": source_id}],
                    "outputs": [{"guid": target_id}],
                    "startTime": datetime.now().isoformat()
                }
            }
            
            # Register process
            response = self.client.entity.create(process)
            guid = response.get('guidAssignments', {}).get(
                process_name
            )
            
            logger.info(f"Created lineage with GUID: {guid}")
            return guid
            
        except Exception as e:
            logger.error(f"Failed to update lineage: {e}")
            raise
    
    def add_classification(
        self,
        entity_guid: str,
        classification: str,
        attributes: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Add classification to entity.
        
        Args:
            entity_guid: Entity GUID
            classification: Classification name
            attributes: Classification attributes
        """
        try:
            logger.info(
                f"Adding classification {classification} "
                f"to {entity_guid}"
            )
            
            self.client.entity.update_classifications(
                entity_guid,
                [
                    {
                        "typeName": classification,
                        "attributes": attributes or {}
                    }
                ]
            )
            
            logger.info("Added classification successfully")
            
        except Exception as e:
            logger.error(f"Failed to add classification: {e}")
            raise 