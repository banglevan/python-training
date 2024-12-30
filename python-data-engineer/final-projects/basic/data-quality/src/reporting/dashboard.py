"""
Quality dashboard module.
"""

from typing import Dict, Any, List
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class QualityDashboard:
    """Quality dashboard management."""
    
    def __init__(self, config: Config):
        """Initialize dashboard."""
        self.config = config
        self.style_config = {
            'figure.figsize': (12, 6),
            'axes.titlesize': 14,
            'axes.labelsize': 12
        }
        plt.style.use('seaborn')
        for key, value in self.style_config.items():
            plt.rcParams[key] = value
    
    def create_quality_trend_plot(
        self,
        metrics_history: List[Dict[str, Any]],
        output_path: str
    ) -> None:
        """
        Create quality trend plot.
        
        Args:
            metrics_history: Historical metrics data
            output_path: Output file path
        """
        try:
            # Prepare data
            df = pd.DataFrame(metrics_history)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Create plot
            fig, ax = plt.subplots()
            
            # Plot quality scores
            for col in df.columns:
                if col.endswith('_score'):
                    ax.plot(
                        df['timestamp'],
                        df[col],
                        label=col.replace('_', ' ').title(),
                        marker='o'
                    )
            
            # Customize plot
            ax.set_title('Data Quality Trends')
            ax.set_xlabel('Date')
            ax.set_ylabel('Score')
            ax.grid(True)
            ax.legend()
            
            # Rotate x-axis labels
            plt.xticks(rotation=45)
            
            # Adjust layout and save
            plt.tight_layout()
            plt.savefig(output_path)
            plt.close()
            
            logger.info(f"Created quality trend plot: {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to create trend plot: {e}")
            raise
    
    def create_issues_summary_plot(
        self,
        check_results: List[Dict[str, Any]],
        output_path: str
    ) -> None:
        """
        Create issues summary plot.
        
        Args:
            check_results: Quality check results
            output_path: Output file path
        """
        try:
            # Count issues by type and status
            issues_df = pd.DataFrame(check_results)
            summary = pd.crosstab(
                issues_df['type'],
                issues_df['status']
            )
            
            # Create plot
            fig, ax = plt.subplots()
            
            # Plot stacked bar chart
            summary.plot(
                kind='bar',
                stacked=True,
                ax=ax,
                color=['red', 'green']
            )
            
            # Customize plot
            ax.set_title('Quality Issues Summary')
            ax.set_xlabel('Check Type')
            ax.set_ylabel('Count')
            
            # Rotate x-axis labels
            plt.xticks(rotation=45)
            
            # Adjust layout and save
            plt.tight_layout()
            plt.savefig(output_path)
            plt.close()
            
            logger.info(f"Created issues summary plot: {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to create issues plot: {e}")
            raise
    
    def generate_html_report(
        self,
        metrics: Dict[str, float],
        trends: Dict[str, Any],
        check_results: List[Dict[str, Any]],
        output_path: str
    ) -> None:
        """
        Generate HTML quality report.
        
        Args:
            metrics: Current metrics
            trends: Trend metrics
            check_results: Quality check results
            output_path: Output file path
        """
        try:
            # Create HTML content
            html_content = f"""
            <html>
            <head>
                <title>Data Quality Report</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    .metric {{ margin: 10px 0; padding: 10px; border: 1px solid #ddd; }}
                    .failed {{ color: red; }}
                    .passed {{ color: green; }}
                </style>
            </head>
            <body>
                <h1>Data Quality Report</h1>
                <p>Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <h2>Quality Metrics</h2>
                <div class="metric">
                    <h3>Overall Quality Score: {metrics['quality_score']:.2%}</h3>
                    {''.join(f'<p>{k}: {v:.2%}</p>' for k, v in metrics.items() if k.endswith('_score'))}
                </div>
                
                <h2>Trends</h2>
                <div class="metric">
                    {''.join(
                        f'''
                        <p>{k}:
                            {v['trend'].upper()}
                            ({v['change']:+.2%} change)
                        </p>
                        '''
                        for k, v in trends.items()
                    )}
                </div>
                
                <h2>Quality Issues</h2>
                {''.join(
                    f'''
                    <div class="metric {r['status']}">
                        <p>Type: {r['type']}</p>
                        <p>Column: {r.get('column', 'N/A')}</p>
                        <p>Score: {r['score']:.2%}</p>
                        <p>Status: {r['status'].upper()}</p>
                    </div>
                    '''
                    for r in check_results
                )}
            </body>
            </html>
            """
            
            # Write HTML file
            with open(output_path, 'w') as f:
                f.write(html_content)
            
            logger.info(f"Generated HTML report: {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to generate HTML report: {e}")
            raise 