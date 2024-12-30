"""
HTML report template module.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
import logging
from .base import BaseTemplate
from src.utils.config import Config

logger = logging.getLogger(__name__)

class HTMLTemplate(BaseTemplate):
    """HTML report template."""
    
    def __init__(self, config: Config):
        """Initialize template."""
        super().__init__(config)
        self.env = Environment(
            loader=FileSystemLoader('src/reporting/templates/html')
        )
        self.template = self.env.get_template('report.html')
    
    def add_section(
        self,
        title: str,
        content: Any,
        type: str = 'text'
    ) -> None:
        """Add section to report."""
        try:
            self.sections.append({
                'title': title,
                'content': content,
                'type': type
            })
            logger.info(f"Added section: {title}")
        except Exception as e:
            logger.error(f"Failed to add section: {e}")
            raise
    
    def add_chart(
        self,
        title: str,
        data: pd.DataFrame,
        chart_type: str,
        **kwargs
    ) -> None:
        """Add chart to report."""
        try:
            if chart_type == 'line':
                fig = px.line(data, **kwargs)
            elif chart_type == 'bar':
                fig = px.bar(data, **kwargs)
            elif chart_type == 'pie':
                fig = px.pie(data, **kwargs)
            else:
                raise ValueError(f"Unsupported chart type: {chart_type}")
            
            self.add_section(
                title,
                fig.to_html(full_html=False),
                'chart'
            )
            logger.info(f"Added chart: {title}")
        except Exception as e:
            logger.error(f"Failed to add chart: {e}")
            raise
    
    def add_table(
        self,
        title: str,
        data: pd.DataFrame,
        **kwargs
    ) -> None:
        """Add table to report."""
        try:
            table_html = data.to_html(
                classes='table table-striped',
                index=False,
                **kwargs
            )
            self.add_section(title, table_html, 'table')
            logger.info(f"Added table: {title}")
        except Exception as e:
            logger.error(f"Failed to add table: {e}")
            raise
    
    def render(self, output_path: str) -> None:
        """Render report to file."""
        try:
            html = self.template.render(
                title=self.config.reports['title'],
                date=datetime.now().strftime('%Y-%m-%d'),
                sections=self.sections
            )
            
            with open(output_path, 'w') as f:
                f.write(html)
            
            logger.info(f"Rendered report to: {output_path}")
        except Exception as e:
            logger.error(f"Failed to render report: {e}")
            raise 