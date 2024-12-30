"""
Tableau integration example.
"""

import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import tableauserverclient as TSC
from pathlib import Path
import yaml
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TableauClient:
    """Tableau Server client."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Tableau client."""
        self.config = config
        self.server_url = config['server_url']
        self.site_id = config.get('site_id', '')
        self.username = config['username']
        self.password = config['password']
        self.project_name = config.get('project_name', 'Default')
        
        # Initialize server client
        self.tableau_auth = TSC.TableauAuth(
            username=self.username,
            password=self.password,
            site_id=self.site_id
        )
        self.server = TSC.Server(self.server_url)
        
        # Sign in
        self.server.auth.sign_in(self.tableau_auth)
    
    def publish_datasource(
        self,
        name: str,
        data: pd.DataFrame,
        extract: bool = True
    ) -> Optional[str]:
        """Publish data source to Tableau Server."""
        try:
            # Save DataFrame to temporary file
            temp_file = Path('temp_datasource.hyper')
            if extract:
                # Save as Hyper extract
                from tableauhyperapi import HyperProcess, Connection, Telemetry
                from tableauhyperapi import CreateMode, TableDefinition, SqlType, TableName
                
                with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                    with Connection(hyper.endpoint, str(temp_file), CreateMode.CREATE_AND_REPLACE) as connection:
                        table_def = TableDefinition(
                            TableName('Extract', 'Data'),
                            [
                                (col, self._get_hyper_type(data[col].dtype))
                                for col in data.columns
                            ]
                        )
                        connection.catalog.create_table(table_def)
                        connection.execute_insert(
                            table_def.table_name,
                            data.values.tolist()
                        )
            else:
                # Save as CSV
                data.to_csv(temp_file, index=False)
            
            # Find project
            project = self._get_project(self.project_name)
            if not project:
                raise ValueError(f"Project not found: {self.project_name}")
            
            # Create datasource item
            datasource = TSC.DatasourceItem(project.id, name)
            
            # Publish
            datasource = self.server.datasources.publish(
                datasource,
                str(temp_file),
                mode=TSC.Server.PublishMode.Overwrite
            )
            
            # Cleanup
            temp_file.unlink()
            
            return datasource.id
            
        except Exception as e:
            logger.error(f"Datasource publication failed: {e}")
            return None
        
    def create_workbook(
        self,
        name: str,
        datasource_id: str,
        views: List[Dict[str, Any]]
    ) -> Optional[str]:
        """Create Tableau workbook."""
        try:
            # Find project
            project = self._get_project(self.project_name)
            if not project:
                raise ValueError(f"Project not found: {self.project_name}")
            
            # Create workbook item
            workbook = TSC.WorkbookItem(project.id, name)
            
            # Add views configuration
            workbook.views = views
            
            # Set datasource
            workbook.datasources = [TSC.DatasourceItem(datasource_id)]
            
            # Publish workbook
            workbook = self.server.workbooks.publish(
                workbook,
                mode=TSC.Server.PublishMode.Overwrite
            )
            
            return workbook.id
            
        except Exception as e:
            logger.error(f"Workbook creation failed: {e}")
            return None
    
    def _get_project(self, name: str) -> Optional[TSC.ProjectItem]:
        """Get project by name."""
        try:
            all_projects, _ = self.server.projects.get()
            for project in all_projects:
                if project.name == name:
                    return project
            return None
            
        except Exception as e:
            logger.error(f"Project retrieval failed: {e}")
            return None
    
    def _get_hyper_type(self, dtype) -> SqlType:
        """Convert pandas dtype to Hyper SQL type."""
        from tableauhyperapi import SqlType
        
        type_mapping = {
            'int64': SqlType.int(),
            'float64': SqlType.double(),
            'bool': SqlType.bool(),
            'datetime64[ns]': SqlType.timestamp(),
            'object': SqlType.text()
        }
        
        return type_mapping.get(str(dtype), SqlType.text())
    
    def close(self):
        """Close Tableau connection."""
        try:
            self.server.auth.sign_out()
        except Exception as e:
            logger.error(f"Tableau sign out failed: {e}")

def main():
    """Main execution."""
    try:
        # Load configuration
        with open('config/tableau.yml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Initialize client
        client = TableauClient(config['tableau'])
        
        # Load sample data
        data = pd.read_sql(
            "SELECT * FROM sync_metrics",
            config['database']['connection_string']
        )
        
        # Publish datasource
        ds_id = client.publish_datasource(
            "Sync Metrics",
            data,
            extract=True
        )
        
        if not ds_id:
            raise RuntimeError("Datasource publication failed")
        
        # Create workbook with views
        views = [
            {
                "name": "Sync Success Rate",
                "type": "text",
                "columns": ["source", "success_rate"],
                "aggregation": "AVG"
            },
            {
                "name": "Sync Latency Trend",
                "type": "line",
                "columns": ["timestamp", "latency"],
                "aggregation": "AVG"
            }
        ]
        
        wb_id = client.create_workbook(
            "Sync Metrics Analysis",
            ds_id,
            views
        )
        
        if wb_id:
            logger.info("Tableau integration completed successfully")
        
    except Exception as e:
        logger.error(f"Tableau integration failed: {e}")
    
    finally:
        client.close()

if __name__ == "__main__":
    main() 