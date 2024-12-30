"""
Power BI integration example.
"""

import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import requests
import msal
import json
from pathlib import Path
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PowerBIClient:
    """Power BI REST API client."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Power BI client."""
        self.config = config
        self.tenant_id = config['tenant_id']
        self.client_id = config['client_id']
        self.client_secret = config['client_secret']
        self.workspace_id = config.get('workspace_id')
        
        # Initialize MSAL app
        self.app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}"
        )
        
        # Get access token
        self.access_token = self._get_token()
        
        # Setup session
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f"Bearer {self.access_token}",
            'Content-Type': 'application/json'
        })
    
    def _get_token(self) -> str:
        """Get access token."""
        try:
            scopes = ['https://analysis.windows.net/powerbi/api/.default']
            result = self.app.acquire_token_silent(
                scopes,
                account=None
            )
            
            if not result:
                result = self.app.acquire_token_for_client(scopes)
            
            if 'access_token' in result:
                return result['access_token']
            else:
                raise ValueError("Failed to acquire token")
                
        except Exception as e:
            logger.error(f"Token acquisition failed: {e}")
            raise
    
    def create_dataset(
        self,
        name: str,
        tables: List[Dict[str, Any]]
    ) -> Optional[str]:
        """Create Power BI dataset."""
        try:
            url = "https://api.powerbi.com/v1.0/myorg"
            if self.workspace_id:
                url += f"/groups/{self.workspace_id}"
            url += "/datasets"
            
            payload = {
                "name": name,
                "defaultMode": "Push",
                "tables": tables
            }
            
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            
            return response.json()['id']
            
        except Exception as e:
            logger.error(f"Dataset creation failed: {e}")
            return None
    
    def push_data(
        self,
        dataset_id: str,
        table_name: str,
        data: pd.DataFrame
    ) -> bool:
        """Push data to Power BI dataset."""
        try:
            url = "https://api.powerbi.com/v1.0/myorg"
            if self.workspace_id:
                url += f"/groups/{self.workspace_id}"
            url += f"/datasets/{dataset_id}/tables/{table_name}/rows"
            
            # Convert DataFrame to records
            records = data.to_dict('records')
            
            # Push data in batches
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                response = self.session.post(url, json={'rows': batch})
                response.raise_for_status()
            
            return True
            
        except Exception as e:
            logger.error(f"Data push failed: {e}")
            return False
    
    def create_report(
        self,
        name: str,
        dataset_id: str,
        definition: Dict[str, Any]
    ) -> Optional[str]:
        """Create Power BI report."""
        try:
            url = "https://api.powerbi.com/v1.0/myorg"
            if self.workspace_id:
                url += f"/groups/{self.workspace_id}"
            url += "/reports"
            
            payload = {
                "name": name,
                "datasetId": dataset_id,
                "definition": definition
            }
            
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            
            return response.json()['id']
            
        except Exception as e:
            logger.error(f"Report creation failed: {e}")
            return None
    
    def refresh_dataset(self, dataset_id: str) -> bool:
        """Refresh Power BI dataset."""
        try:
            url = "https://api.powerbi.com/v1.0/myorg"
            if self.workspace_id:
                url += f"/groups/{self.workspace_id}"
            url += f"/datasets/{dataset_id}/refreshes"
            
            response = self.session.post(url)
            response.raise_for_status()
            
            return True
            
        except Exception as e:
            logger.error(f"Dataset refresh failed: {e}")
            return False

def main():
    """Main execution."""
    try:
        # Load configuration
        with open('config/powerbi.yml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Initialize client
        client = PowerBIClient(config['powerbi'])
        
        # Load sample data
        data = pd.read_sql(
            "SELECT * FROM sync_metrics",
            config['database']['connection_string']
        )
        
        # Define dataset schema
        tables = [{
            "name": "SyncMetrics",
            "columns": [
                {"name": "metric_id", "dataType": "string"},
                {"name": "timestamp", "dataType": "datetime"},
                {"name": "source", "dataType": "string"},
                {"name": "metric_name", "dataType": "string"},
                {"name": "metric_value", "dataType": "double"},
                {"name": "status", "dataType": "string"}
            ]
        }]
        
        # Create dataset
        dataset_id = client.create_dataset("Sync Metrics", tables)
        
        if not dataset_id:
            raise RuntimeError("Dataset creation failed")
        
        # Push data
        success = client.push_data(
            dataset_id,
            "SyncMetrics",
            data
        )
        
        if not success:
            raise RuntimeError("Data push failed")
        
        # Create report
        report_definition = {
            "version": "1.0",
            "pages": [{
                "name": "Sync Metrics Overview",
                "visuals": [
                    {
                        "type": "card",
                        "title": "Success Rate",
                        "measure": "AVERAGE(SyncMetrics[metric_value])"
                    },
                    {
                        "type": "lineChart",
                        "title": "Latency Trend",
                        "xAxis": "SyncMetrics[timestamp]",
                        "yAxis": "SyncMetrics[metric_value]"
                    }
                ]
            }]
        }
        
        report_id = client.create_report(
            "Sync Metrics Report",
            dataset_id,
            report_definition
        )
        
        if report_id:
            logger.info("Power BI integration completed successfully")
        
    except Exception as e:
        logger.error(f"Power BI integration failed: {e}")

if __name__ == "__main__":
    main() 