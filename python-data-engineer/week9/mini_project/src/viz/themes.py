"""
Visualization themes module.
"""

import logging
from typing import Dict, Any
import plotly.graph_objects as go
import plotly.io as pio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ThemeManager:
    """Visualization theme manager."""
    
    # Default theme configurations
    THEMES = {
        'light': {
            'background_color': '#ffffff',
            'paper_color': '#ffffff',
            'font_color': '#2f2f2f',
            'grid_color': '#e0e0e0',
            'colorway': [
                '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728',
                '#9467bd', '#8c564b', '#e377c2', '#7f7f7f'
            ]
        },
        'dark': {
            'background_color': '#282c34',
            'paper_color': '#282c34',
            'font_color': '#ffffff',
            'grid_color': '#3f4451',
            'colorway': [
                '#61dafb', '#ff6b6b', '#5fd35f', '#ffd93d',
                '#c678dd', '#ff9f43', '#ff78c6', '#abb2bf'
            ]
        },
        'corporate': {
            'background_color': '#f8f9fa',
            'paper_color': '#ffffff',
            'font_color': '#343a40',
            'grid_color': '#dee2e6',
            'colorway': [
                '#007bff', '#28a745', '#dc3545', '#ffc107',
                '#17a2b8', '#6610f2', '#e83e8c', '#6c757d'
            ]
        }
    }
    
    def __init__(self):
        """Initialize theme manager."""
        self.current_theme = 'light'
        self._register_themes()
    
    def _register_themes(self):
        """Register custom themes with plotly."""
        try:
            for theme_name, theme_config in self.THEMES.items():
                template = go.layout.Template()
                
                # Set layout defaults
                template.layout.update(
                    plot_bgcolor=theme_config['background_color'],
                    paper_bgcolor=theme_config['paper_color'],
                    font={'color': theme_config['font_color']},
                    xaxis={
                        'gridcolor': theme_config['grid_color'],
                        'zerolinecolor': theme_config['grid_color']
                    },
                    yaxis={
                        'gridcolor': theme_config['grid_color'],
                        'zerolinecolor': theme_config['grid_color']
                    },
                    colorway=theme_config['colorway']
                )
                
                # Register template
                pio.templates[theme_name] = template
                
            logger.info("Themes registered successfully")
            
        except Exception as e:
            logger.error(f"Theme registration failed: {e}")
            raise
    
    def set_theme(self, theme_name: str) -> bool:
        """Set current theme."""
        try:
            if theme_name not in self.THEMES:
                raise ValueError(f"Unknown theme: {theme_name}")
            
            self.current_theme = theme_name
            return True
            
        except Exception as e:
            logger.error(f"Theme setting failed: {e}")
            return False
    
    def get_theme(self) -> Dict[str, Any]:
        """Get current theme configuration."""
        try:
            return self.THEMES[self.current_theme]
        except Exception as e:
            logger.error(f"Theme retrieval failed: {e}")
            return {}
    
    def get_available_themes(self) -> Dict[str, Dict[str, Any]]:
        """Get all available themes."""
        return self.THEMES
    
    def create_custom_theme(
        self,
        name: str,
        config: Dict[str, Any]
    ) -> bool:
        """Create custom theme."""
        try:
            required_keys = [
                'background_color',
                'paper_color',
                'font_color',
                'grid_color',
                'colorway'
            ]
            
            # Validate config
            if not all(key in config for key in required_keys):
                raise ValueError("Missing required theme configuration")
            
            # Add theme
            self.THEMES[name] = config
            self._register_themes()
            
            return True
            
        except Exception as e:
            logger.error(f"Custom theme creation failed: {e}")
            return False

# Global theme manager instance
theme_manager = ThemeManager() 