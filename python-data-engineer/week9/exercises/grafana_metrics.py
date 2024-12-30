"""
Grafana metrics integration example.
"""

import logging
from typing import Dict, Any, List, Optional
import requests
import json
from datetime import datetime, timedelta
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GrafanaClient:
    """Grafana API client."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Grafana client."""
        self.config = config
        self.base_url = config['base_url'].rstrip('/')
        self.api_key = config['api_key']
        self.org_id = config.get('org_id', 1)
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f"Bearer {self.api_key}",
            'Content-Type': 'application/json'
        })
    
    def create_datasource(
        self,
        name: str,
        type: str,
        url: str,
        database: str,
        user: str,
        password: str,
        access: str = 'proxy'
    ) -> Optional[int]:
        """Create data source."""
        try:
            payload = {
                "name": name,
                "type": type,
                "url": url,
                "access": access,
                "database": database,
                "user": user,
                "secureJsonData": {
                    "password": password
                },
                "jsonData": {
                    "sslmode": "disable",
                    "postgresVersion": 1200,
                    "timescaledb": False
                }
            }
            
            response = self.session.post(
                f"{self.base_url}/api/datasources",
                json=payload
            )
            
            if response.status_code == 200:
                return response.json()['datasource']['id']
            
            logger.error(f"Datasource creation failed: {response.text}")
            return None
            
        except Exception as e:
            logger.error(f"Datasource creation failed: {e}")
            return None
    
    def create_dashboard(
        self,
        title: str,
        panels: List[Dict[str, Any]],
        time_from: str = "now-6h",
        time_to: str = "now"
    ) -> Optional[str]:
        """Create dashboard."""
        try:
            payload = {
                "dashboard": {
                    "title": title,
                    "panels": panels,
                    "time": {
                        "from": time_from,
                        "to": time_to
                    },
                    "timezone": "browser",
                    "schemaVersion": 21,
                    "version": 0
                },
                "overwrite": True
            }
            
            response = self.session.post(
                f"{self.base_url}/api/dashboards/db",
                json=payload
            )
            
            if response.status_code == 200:
                return response.json()['uid']
            
            logger.error(f"Dashboard creation failed: {response.text}")
            return None
            
        except Exception as e:
            logger.error(f"Dashboard creation failed: {e}")
            return None
    
    def create_alert_rule(
        self,
        name: str,
        query: Dict[str, Any],
        condition: Dict[str, Any],
        notification_channel_id: int
    ) -> Optional[int]:
        """Create alert rule."""
        try:
            payload = {
                "name": name,
                "type": "alerting",
                "message": f"Alert: {name}",
                "alertRuleTags": {},
                "conditions": [condition],
                "notifications": [{
                    "uid": str(notification_channel_id)
                }],
                "executionErrorState": "alerting",
                "noDataState": "no_data",
                "evalType": "last",
                "queries": [query]
            }
            
            response = self.session.post(
                f"{self.base_url}/api/alert-rules",
                json=payload
            )
            
            if response.status_code == 200:
                return response.json()['id']
            
            logger.error(f"Alert rule creation failed: {response.text}")
            return None
            
        except Exception as e:
            logger.error(f"Alert rule creation failed: {e}")
            return None
    
    def create_notification_channel(
        self,
        name: str,
        type: str,
        settings: Dict[str, Any]
    ) -> Optional[int]:
        """Create notification channel."""
        try:
            payload = {
                "name": name,
                "type": type,
                "settings": settings
            }
            
            response = self.session.post(
                f"{self.base_url}/api/alert-notifications",
                json=payload
            )
            
            if response.status_code == 200:
                return response.json()['id']
            
            logger.error(f"Channel creation failed: {response.text}")
            return None
            
        except Exception as e:
            logger.error(f"Channel creation failed: {e}")
            return None

def main():
    """Main execution."""
    try:
        # Load configuration
        with open('config/grafana.yml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Initialize client
        client = GrafanaClient(config['grafana'])
        
        # Create PostgreSQL data source
        ds_id = client.create_datasource(
            "Sync Metrics",
            "postgres",
            config['database']['host'],
            config['database']['dbname'],
            config['database']['user'],
            config['database']['password']
        )
        
        if not ds_id:
            raise RuntimeError("Datasource creation failed")
        
        # Create notification channel
        channel_id = client.create_notification_channel(
            "Sync Alerts",
            "email",
            {
                "addresses": config['alerts']['email']
            }
        )
        
        if not channel_id:
            raise RuntimeError("Notification channel creation failed")
        
        # Create dashboard panels
        panels = [
            # Sync Success Rate
            {
                "title": "Sync Success Rate",
                "type": "gauge",
                "gridPos": {"x": 0, "y": 0, "w": 6, "h": 4},
                "targets": [{
                    "rawSql": """
                    SELECT 
                        COUNT(CASE WHEN status = 'success' THEN 1 END) * 100.0 / COUNT(*)
                    FROM sync_metrics
                    WHERE $__timeFilter(timestamp)
                    """,
                    "format": "time_series"
                }]
            },
            # Sync Latency
            {
                "title": "Sync Latency",
                "type": "graph",
                "gridPos": {"x": 6, "y": 0, "w": 18, "h": 8},
                "targets": [{
                    "rawSql": """
                    SELECT
                        timestamp as time,
                        metric_value as value
                    FROM sync_metrics
                    WHERE metric_name = 'sync_latency'
                        AND $__timeFilter(timestamp)
                    ORDER BY timestamp
                    """,
                    "format": "time_series"
                }]
            }
        ]
        
        # Create dashboard
        dashboard_uid = client.create_dashboard(
            "Sync Metrics Dashboard",
            panels
        )
        
        if not dashboard_uid:
            raise RuntimeError("Dashboard creation failed")
        
        # Create alert rule
        alert_id = client.create_alert_rule(
            "High Sync Latency",
            {
                "refId": "A",
                "datasourceUid": str(ds_id),
                "model": {
                    "rawSql": """
                    SELECT AVG(metric_value)
                    FROM sync_metrics
                    WHERE metric_name = 'sync_latency'
                        AND $__timeFilter(timestamp)
                    """
                }
            },
            {
                "evaluator": {
                    "type": "gt",
                    "params": [5.0]  # Alert if latency > 5s
                },
                "operator": {
                    "type": "and"
                },
                "query": {
                    "params": ["A", "5m", "now"]
                },
                "reducer": {
                    "type": "avg",
                    "params": []
                }
            },
            channel_id
        )
        
        if alert_id:
            logger.info("Grafana setup completed successfully")
        
    except Exception as e:
        logger.error(f"Grafana integration failed: {e}")

if __name__ == "__main__":
    main() 