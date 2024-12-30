"""
Visualization tests.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import plotly.graph_objects as go

from src.viz.charts import (
    LineChart,
    BarChart,
    PieChart,
    ChartFactory
)
from src.viz.dashboards import Dashboard, DashboardManager
from src.viz.themes import ThemeManager

class TestCharts(unittest.TestCase):
    """Test chart components."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_data = pd.DataFrame({
            'x': range(5),
            'y1': [10, 20, 30, 40, 50],
            'y2': [15, 25, 35, 45, 55]
        })
    
    def test_line_chart(self):
        """Test line chart rendering."""
        # Setup
        config = {
            'title': 'Test Chart',
            'x_column': 'x',
            'y_columns': ['y1', 'y2']
        }
        chart = LineChart(config)
        
        # Execute
        result = chart.render(self.test_data)
        
        # Verify
        self.assertEqual(result['type'], 'line')
        self.assertIn('data', result)
    
    def test_chart_factory(self):
        """Test chart factory."""
        # Execute and verify
        chart = ChartFactory.create_chart({
            'type': 'line',
            'title': 'Test'
        })
        self.assertIsInstance(chart, LineChart)
        
        with self.assertRaises(ValueError):
            ChartFactory.create_chart({'type': 'invalid'})

class TestDashboards(unittest.TestCase):
    """Test dashboard management."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.dashboard_config = {
            'name': 'Test Dashboard',
            'layout': {'type': 'grid'},
            'charts': [{
                'id': 'chart1',
                'type': 'line',
                'title': 'Test Chart'
            }]
        }
    
    def test_dashboard_creation(self):
        """Test dashboard creation."""
        # Setup
        dashboard = Dashboard(self.dashboard_config)
        
        # Execute
        dashboard.update_data('chart1', pd.DataFrame({
            'x': range(3),
            'y': [10, 20, 30]
        }))
        result = dashboard.render()
        
        # Verify
        self.assertEqual(result['name'], 'Test Dashboard')
        self.assertIn('charts', result)
    
    @patch('src.core.database.DatabaseManager.get_db')
    def test_dashboard_manager(self, mock_db):
        """Test dashboard manager."""
        # Setup
        manager = DashboardManager()
        mock_db.return_value.__enter__.return_value = Mock()
        
        # Execute
        dashboard_id = manager.create_dashboard(
            self.dashboard_config,
            {'id': 1, 'role': 'admin'}
        )
        
        # Verify
        self.assertIsNotNone(dashboard_id)

class TestThemes(unittest.TestCase):
    """Test theme management."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.theme_manager = ThemeManager()
    
    def test_theme_operations(self):
        """Test theme operations."""
        # Test default theme
        self.assertEqual(self.theme_manager.current_theme, 'light')
        
        # Test theme switching
        success = self.theme_manager.set_theme('dark')
        self.assertTrue(success)
        self.assertEqual(self.theme_manager.current_theme, 'dark')
        
        # Test custom theme creation
        success = self.theme_manager.create_custom_theme(
            'custom',
            {
                'background_color': '#ffffff',
                'paper_color': '#ffffff',
                'font_color': '#000000',
                'grid_color': '#e0e0e0',
                'colorway': ['#000000']
            }
        )
        self.assertTrue(success)
        self.assertIn('custom', self.theme_manager.get_available_themes())

if __name__ == '__main__':
    unittest.main() 