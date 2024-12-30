"""
Dashboard management module.
"""

import logging
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime

from ..core.database import db_manager
from ..core.security import security_manager
from .charts import ChartFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Dashboard:
    """Dashboard manager."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize dashboard."""
        self.config = config
        self.id = config.get('id')
        self.name = config.get('name', 'Unnamed Dashboard')
        self.layout = config.get('layout', {})
        self.charts = {}
        self.data_cache = {}
        
        # Initialize charts
        self._init_charts()
    
    def _init_charts(self):
        """Initialize chart components."""
        try:
            for chart_config in self.config.get('charts', []):
                chart_id = chart_config['id']
                self.charts[chart_id] = ChartFactory.create_chart(chart_config)
                
        except Exception as e:
            logger.error(f"Chart initialization failed: {e}")
            raise
    
    def update_data(self, chart_id: str, data: pd.DataFrame):
        """Update chart data."""
        try:
            self.data_cache[chart_id] = data
        except Exception as e:
            logger.error(f"Data update failed: {e}")
            raise
    
    def render(self) -> Dict[str, Any]:
        """Render dashboard."""
        try:
            rendered_charts = {}
            
            for chart_id, chart in self.charts.items():
                if chart_id in self.data_cache:
                    rendered_charts[chart_id] = chart.render(
                        self.data_cache[chart_id]
                    )
            
            return {
                'id': self.id,
                'name': self.name,
                'layout': self.layout,
                'charts': rendered_charts,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Dashboard rendering failed: {e}")
            raise

class DashboardManager:
    """Dashboard management system."""
    
    def __init__(self):
        """Initialize dashboard manager."""
        self.dashboards: Dict[str, Dashboard] = {}
    
    def create_dashboard(
        self,
        config: Dict[str, Any],
        user: Dict[str, Any]
    ) -> Optional[str]:
        """Create new dashboard."""
        try:
            # Validate permissions
            if not security_manager.check_permissions('editor', user['role']):
                raise PermissionError("Insufficient permissions")
            
            # Create dashboard
            dashboard = Dashboard(config)
            self.dashboards[dashboard.id] = dashboard
            
            # Save to database
            with db_manager.get_db() as db:
                db.execute(
                    """
                    INSERT INTO dashboards (id, name, config, owner_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (dashboard.id, dashboard.name, config, user['id'])
                )
                db.commit()
            
            return dashboard.id
            
        except Exception as e:
            logger.error(f"Dashboard creation failed: {e}")
            return None
    
    def get_dashboard(
        self,
        dashboard_id: str,
        user: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Get dashboard by ID."""
        try:
            if dashboard_id not in self.dashboards:
                return None
            
            dashboard = self.dashboards[dashboard_id]
            
            # Check permissions
            with db_manager.get_db() as db:
                result = db.execute(
                    """
                    SELECT owner_id FROM dashboards
                    WHERE id = %s
                    """,
                    (dashboard_id,)
                ).fetchone()
                
                if not (
                    security_manager.check_permissions('admin', user['role']) or
                    result['owner_id'] == user['id']
                ):
                    raise PermissionError("Insufficient permissions")
            
            return dashboard.render()
            
        except Exception as e:
            logger.error(f"Dashboard retrieval failed: {e}")
            return None
    
    def update_dashboard(
        self,
        dashboard_id: str,
        config: Dict[str, Any],
        user: Dict[str, Any]
    ) -> bool:
        """Update dashboard configuration."""
        try:
            if dashboard_id not in self.dashboards:
                return False
            
            # Check permissions
            with db_manager.get_db() as db:
                result = db.execute(
                    """
                    SELECT owner_id FROM dashboards
                    WHERE id = %s
                    """,
                    (dashboard_id,)
                ).fetchone()
                
                if not (
                    security_manager.check_permissions('admin', user['role']) or
                    result['owner_id'] == user['id']
                ):
                    raise PermissionError("Insufficient permissions")
                
                # Update dashboard
                self.dashboards[dashboard_id] = Dashboard(config)
                
                # Save to database
                db.execute(
                    """
                    UPDATE dashboards
                    SET name = %s, config = %s
                    WHERE id = %s
                    """,
                    (config['name'], config, dashboard_id)
                )
                db.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"Dashboard update failed: {e}")
            return False
    
    def delete_dashboard(
        self,
        dashboard_id: str,
        user: Dict[str, Any]
    ) -> bool:
        """Delete dashboard."""
        try:
            if dashboard_id not in self.dashboards:
                return False
            
            # Check permissions
            with db_manager.get_db() as db:
                result = db.execute(
                    """
                    SELECT owner_id FROM dashboards
                    WHERE id = %s
                    """,
                    (dashboard_id,)
                ).fetchone()
                
                if not (
                    security_manager.check_permissions('admin', user['role']) or
                    result['owner_id'] == user['id']
                ):
                    raise PermissionError("Insufficient permissions")
                
                # Delete from database
                db.execute(
                    "DELETE FROM dashboards WHERE id = %s",
                    (dashboard_id,)
                )
                db.commit()
                
                # Remove from memory
                del self.dashboards[dashboard_id]
            
            return True
            
        except Exception as e:
            logger.error(f"Dashboard deletion failed: {e}")
            return False

# Global dashboard manager instance
dashboard_manager = DashboardManager() 