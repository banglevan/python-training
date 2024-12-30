"""
Chart components module.
"""

import logging
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Chart(ABC):
    """Abstract chart base class."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize chart."""
        self.config = config
        self.title = config.get('title', '')
        self.x_axis = config.get('x_axis', {})
        self.y_axis = config.get('y_axis', {})
        self.theme = config.get('theme', 'plotly')
    
    @abstractmethod
    def render(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Render chart."""
        pass
    
    def _apply_layout(self, fig: go.Figure) -> go.Figure:
        """Apply common layout settings."""
        fig.update_layout(
            title=self.title,
            template=self.theme,
            xaxis_title=self.x_axis.get('title'),
            yaxis_title=self.y_axis.get('title'),
            showlegend=self.config.get('show_legend', True),
            height=self.config.get('height', 400),
            width=self.config.get('width', 600)
        )
        return fig

class LineChart(Chart):
    """Line chart component."""
    
    def render(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Render line chart."""
        try:
            fig = go.Figure()
            
            # Add traces
            for column in self.config.get('y_columns', []):
                fig.add_trace(
                    go.Scatter(
                        x=data[self.config['x_column']],
                        y=data[column],
                        name=column,
                        mode='lines+markers'
                    )
                )
            
            # Apply layout
            fig = self._apply_layout(fig)
            
            return {
                'type': 'line',
                'data': json.loads(fig.to_json())
            }
            
        except Exception as e:
            logger.error(f"Line chart rendering failed: {e}")
            raise

class BarChart(Chart):
    """Bar chart component."""
    
    def render(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Render bar chart."""
        try:
            orientation = self.config.get('orientation', 'v')
            
            if orientation == 'v':
                fig = go.Figure(
                    go.Bar(
                        x=data[self.config['x_column']],
                        y=data[self.config['y_column']],
                        name=self.config.get('series_name', '')
                    )
                )
            else:
                fig = go.Figure(
                    go.Bar(
                        y=data[self.config['y_column']],
                        x=data[self.config['x_column']],
                        orientation='h',
                        name=self.config.get('series_name', '')
                    )
                )
            
            # Apply layout
            fig = self._apply_layout(fig)
            
            return {
                'type': 'bar',
                'data': json.loads(fig.to_json())
            }
            
        except Exception as e:
            logger.error(f"Bar chart rendering failed: {e}")
            raise

class PieChart(Chart):
    """Pie chart component."""
    
    def render(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Render pie chart."""
        try:
            fig = go.Figure(
                go.Pie(
                    labels=data[self.config['label_column']],
                    values=data[self.config['value_column']],
                    hole=self.config.get('donut_hole', 0)
                )
            )
            
            # Apply layout
            fig = self._apply_layout(fig)
            
            return {
                'type': 'pie',
                'data': json.loads(fig.to_json())
            }
            
        except Exception as e:
            logger.error(f"Pie chart rendering failed: {e}")
            raise

class ScatterPlot(Chart):
    """Scatter plot component."""
    
    def render(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Render scatter plot."""
        try:
            fig = go.Figure(
                go.Scatter(
                    x=data[self.config['x_column']],
                    y=data[self.config['y_column']],
                    mode='markers',
                    marker=dict(
                        size=self.config.get('marker_size', 10),
                        color=data[self.config.get('color_column')] if 'color_column' in self.config else None
                    ),
                    text=data[self.config.get('hover_column')] if 'hover_column' in self.config else None
                )
            )
            
            # Apply layout
            fig = self._apply_layout(fig)
            
            return {
                'type': 'scatter',
                'data': json.loads(fig.to_json())
            }
            
        except Exception as e:
            logger.error(f"Scatter plot rendering failed: {e}")
            raise

class ChartFactory:
    """Chart factory."""
    
    @staticmethod
    def create_chart(chart_config: Dict[str, Any]) -> Chart:
        """Create chart instance."""
        chart_type = chart_config['type'].lower()
        
        if chart_type == 'line':
            return LineChart(chart_config)
        elif chart_type == 'bar':
            return BarChart(chart_config)
        elif chart_type == 'pie':
            return PieChart(chart_config)
        elif chart_type == 'scatter':
            return ScatterPlot(chart_config)
        else:
            raise ValueError(f"Unsupported chart type: {chart_type}") 