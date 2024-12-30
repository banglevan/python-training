"""
Graph storage management using Neo4j.
"""

from typing import Dict, Any, Optional, List, Union
import logging
from neo4j import GraphDatabase
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GraphManager:
    """Graph storage management."""
    
    def __init__(
        self,
        uri: str,
        username: str,
        password: str
    ):
        """Initialize manager."""
        self.driver = GraphDatabase.driver(
            uri,
            auth=(username, password)
        )
        self.indexes = {}
    
    def close(self):
        """Close connection."""
        self.driver.close()
    
    def create_node(
        self,
        label: str,
        properties: Dict[str, Any]
    ) -> str:
        """
        Create node.
        
        Args:
            label: Node label
            properties: Node properties
        
        Returns:
            Node ID
        """
        try:
            with self.driver.session() as session:
                result = session.write_transaction(
                    self._create_node,
                    label,
                    properties
                )
                logger.info(f"Created node: {result}")
                return result
                
        except Exception as e:
            logger.error(f"Failed to create node: {e}")
            raise
    
    def create_relationship(
        self,
        start_node: str,
        end_node: str,
        rel_type: str,
        properties: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Create relationship.
        
        Args:
            start_node: Start node ID
            end_node: End node ID
            rel_type: Relationship type
            properties: Relationship properties
        """
        try:
            with self.driver.session() as session:
                session.write_transaction(
                    self._create_relationship,
                    start_node,
                    end_node,
                    rel_type,
                    properties or {}
                )
                logger.info(
                    f"Created relationship: {start_node}-[{rel_type}]->{end_node}"
                )
                
        except Exception as e:
            logger.error(f"Failed to create relationship: {e}")
            raise
    
    def create_index(
        self,
        label: str,
        properties: List[str]
    ) -> None:
        """
        Create index.
        
        Args:
            label: Node label
            properties: Properties to index
        """
        try:
            with self.driver.session() as session:
                index_name = f"idx_{label}_{'_'.join(properties)}"
                
                session.run(
                    f"""
                    CREATE INDEX {index_name} IF NOT EXISTS
                    FOR (n:{label})
                    ON ({', '.join(f'n.{p}' for p in properties)})
                    """
                )
                
                self.indexes[index_name] = {
                    'label': label,
                    'properties': properties,
                    'created_at': datetime.now().isoformat()
                }
                
                logger.info(f"Created index: {index_name}")
                
        except Exception as e:
            logger.error(f"Failed to create index: {e}")
            raise
    
    def query(
        self,
        cypher: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute Cypher query.
        
        Args:
            cypher: Cypher query
            parameters: Query parameters
        
        Returns:
            Query results
        """
        try:
            with self.driver.session() as session:
                result = session.run(cypher, parameters or {})
                return [dict(record) for record in result]
                
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    @staticmethod
    def _create_node(
        tx,
        label: str,
        properties: Dict[str, Any]
    ) -> str:
        """Create node transaction."""
        result = tx.run(
            f"""
            CREATE (n:{label} $properties)
            RETURN id(n) as node_id
            """,
            properties=properties
        )
        return result.single()["node_id"]
    
    @staticmethod
    def _create_relationship(
        tx,
        start_node: str,
        end_node: str,
        rel_type: str,
        properties: Dict[str, Any]
    ) -> None:
        """Create relationship transaction."""
        tx.run(
            f"""
            MATCH (a), (b)
            WHERE id(a) = $start AND id(b) = $end
            CREATE (a)-[r:{rel_type} $properties]->(b)
            """,
            start=start_node,
            end=end_node,
            properties=properties
        ) 