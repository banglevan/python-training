"""
Apache Superset integration example.
"""

import logging
from typing import Dict, Any, List, Optional
import requests
import json
import pandas as pd
from sqlalchemy import create_engine
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SupersetClient:
    """Apache Superset API client."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Superset client."""
        self.config = config
        self.base_url = config['base_url'].rstrip('/')
        self.username = config['username']
        self.password = config['password']
        self.session = requests.Session()
        self.access_token = None
        
        # Initialize connection
        self._login()
    
    def _login(self) -> bool:
        """Login to Superset."""
        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/security/login",
                json={
                    "username": self.username,
                    "password": self.password,
                    "provider": "db"
                }
            )
            
            if response.status_code == 200:
                self.access_token = response.json()['access_token']
                self.session.headers.update({
                    'Authorization': f"Bearer {self.access_token}"
                })
                return True
            
            logger.error(f"Login failed: {response.text}")
            return False
            
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False
    
    def create_database(
        self,
        database_name: str,
        sqlalchemy_uri: str,
        extra: Optional[Dict[str, Any]] = None
    ) -> Optional[int]:
        """Create database connection."""
        try:
            payload = {
                "database_name": database_name,
                "sqlalchemy_uri": sqlalchemy_uri,
                "extra": json.dumps(extra or {})
            }
            
            response = self.session.post(
                f"{self.base_url}/api/v1/database/",
                json=payload
            )
            
            if response.status_code == 201:
                return response.json()['id']
            
            logger.error(f"Database creation failed: {response.text}")
            return None
            
        except Exception as e:
            logger.error(f"Database creation failed: {e}")
            return None
    
    def create_dataset(
        self,
        database_id: int,
        schema: str,
        table: str,
        dataset_name: Optional[str] = None
    ) -> Optional[int]:
        """Create dataset from database table."""
        try:
            payload = {
                "database": database_id,
                "schema": schema,
                "table_name": table,
                "sql": f"SELECT * FROM {schema}.{table}",
                "datasource_name": dataset_name or table
            }
            
            response = self.session.post(
                f"{self.base_url}/api/v1/dataset/",
                json=payload
            )
            
            if response.status_code == 201:
                return response.json()['id']
            
            logger.error(f"Dataset creation failed: {response.text}")
            return None
            
        except Exception as e:
            logger.error(f"Dataset creation failed: {e}")
            return None
    
    def create_chart(
        self,
        dataset_id: int,
        chart_name: str,
        viz_type: str,
        metrics: List[Dict[str, Any]],
        groupby: Optional[List[str]] = None,
        filters: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[int]:
        """Create visualization chart."""
        try:
            payload = {
                "datasource_id": dataset_id,
                "datasource_type": "table",
                "viz_type": viz_type,
                "params": {
                    "metrics": metrics,
                    "groupby": groupby or [],
                    "adhoc_filters": filters or [],
                    "viz_type": viz_type
                },
                "slice_name": chart_name
            }
            
            response = self.session.post(
                f"{self.base_url}/api/v1/chart/",
                json=payload
            )
            
            if response.status_code == 201:
                return response.json()['id']
            
            logger.error(f"Chart creation failed: {response.text}")
            return None
            
        except Exception as e:
            logger.error(f"Chart creation failed: {e}")
            return None
    
    def create_dashboard(
        self,
        dashboard_title: str,
        position_json: Dict[str, Any]
    ) -> Optional[int]:
        """Create dashboard."""
        try:
            payload = {
                "dashboard_title": dashboard_title,
                "position_json": json.dumps(position_json),
                "published": True
            }
            
            response = self.session.post(
                f"{self.base_url}/api/v1/dashboard/",
                json=payload
            )
            
            if response.status_code == 201:
                return response.json()['id']
            
            logger.error(f"Dashboard creation failed: {response.text}")
            return None
            
        except Exception as e:
            logger.error(f"Dashboard creation failed: {e}")
            return None
    
    def add_charts_to_dashboard(
        self,
        dashboard_id: int,
        chart_ids: List[int]
    ) -> bool:
        """Add charts to dashboard."""
        try:
            payload = {
                "override_slice_ids": chart_ids
            }
            
            response = self.session.put(
                f"{self.base_url}/api/v1/dashboard/{dashboard_id}",
                json=payload
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Adding charts failed: {e}")
            return False

def main():
    """Main execution."""
    try:
        # Load configuration
        with open('config/superset.yml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Initialize client
        client = SupersetClient(config['superset'])
        
        # Create database connection
        db_id = client.create_database(
            "Sync Metrics DB",
            config['database']['sqlalchemy_uri']
        )
        
        if not db_id:
            raise RuntimeError("Database creation failed")
        
        # Create datasets
        sync_metrics_id = client.create_dataset(
            db_id,
            "public",
            "sync_metrics",
            "Sync Metrics"
        )
        
        if not sync_metrics_id:
            raise RuntimeError("Dataset creation failed")
        
        # Create charts
        charts = []
        
        # Sync events by source
        charts.append(
            client.create_chart(
                sync_metrics_id,
                "Sync Events by Source",
                "pie",
                [{
                    "aggregate": "COUNT",
                    "column": {"column_name": "metric_id"}
                }],
                groupby=["source"]
            )
        )
        
        # Sync latency over time
        charts.append(
            client.create_chart(
                sync_metrics_id,
                "Sync Latency Trend",
                "line",
                [{
                    "aggregate": "AVG",
                    "column": {"column_name": "metric_value"}
                }],
                groupby=["timestamp"]
            )
        )
        
        # Create dashboard
        dashboard_id = client.create_dashboard(
            "Sync Metrics Overview",
            {
                "CHART-1": {
                    "type": "CHART",
                    "id": charts[0],
                    "position": {"x": 0, "y": 0, "width": 6, "height": 4}
                },
                "CHART-2": {
                    "type": "CHART",
                    "id": charts[1],
                    "position": {"x": 6, "y": 0, "width": 6, "height": 4}
                }
            }
        )
        
        if dashboard_id:
            logger.info("Dashboard created successfully")
        
    except Exception as e:
        logger.error(f"Superset integration failed: {e}")

if __name__ == "__main__":
    main() 