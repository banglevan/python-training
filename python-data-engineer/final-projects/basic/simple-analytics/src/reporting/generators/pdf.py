"""
PDF report generator module.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.platypus import Spacer, Image, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from datetime import datetime
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class PDFGenerator:
    """PDF report generator."""
    
    def __init__(self, config: Config):
        """Initialize generator."""
        self.config = config
        self.styles = getSampleStyleSheet()
        self._setup_styles()
    
    def _setup_styles(self):
        """Set up custom styles."""
        # Title style
        self.styles.add(
            ParagraphStyle(
                name='CustomTitle',
                parent=self.styles['Heading1'],
                fontSize=24,
                spaceAfter=30
            )
        )
        
        # Section style
        self.styles.add(
            ParagraphStyle(
                name='Section',
                parent=self.styles['Heading2'],
                fontSize=16,
                spaceAfter=20
            )
        )
        
        # Table header style
        self.styles.add(
            ParagraphStyle(
                name='TableHeader',
                parent=self.styles['Normal'],
                fontSize=12,
                textColor=colors.white
            )
        )
    
    def generate_sales_report(
        self,
        metrics: pd.DataFrame,
        trends: Dict[str, Any],
        output_path: str,
        title: str = "Sales Report"
    ) -> None:
        """
        Generate sales report PDF.
        
        Args:
            metrics: Sales metrics DataFrame
            trends: Trend analysis results
            output_path: Output file path
            title: Report title
        """
        try:
            doc = SimpleDocTemplate(
                output_path,
                pagesize=letter,
                rightMargin=72,
                leftMargin=72,
                topMargin=72,
                bottomMargin=72
            )
            
            # Build content
            content = []
            
            # Title
            content.append(
                Paragraph(
                    f"{title}<br/><font size=12>{datetime.now().strftime('%Y-%m-%d')}</font>",
                    self.styles['CustomTitle']
                )
            )
            
            # Summary section
            content.extend(self._create_summary_section(metrics, trends))
            content.append(PageBreak())
            
            # Metrics section
            content.extend(self._create_metrics_section(metrics))
            content.append(PageBreak())
            
            # Trends section
            content.extend(self._create_trends_section(trends))
            
            # Generate PDF
            doc.build(content)
            logger.info(f"Generated sales report: {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to generate sales report: {e}")
            raise
    
    def _create_summary_section(
        self,
        metrics: pd.DataFrame,
        trends: Dict[str, Any]
    ) -> List:
        """Create summary section."""
        content = []
        
        # Section title
        content.append(
            Paragraph("Executive Summary", self.styles['Section'])
        )
        content.append(Spacer(1, 12))
        
        # Key metrics
        latest = metrics.iloc[-1]
        summary_data = [
            ["Metric", "Current", "Change"],
            ["Revenue", f"${latest['total_revenue']:,.2f}",
             f"{trends['revenue']['change']:+.1%}"],
            ["Transactions", f"{latest['transaction_count']:,}",
             f"{trends['transaction_count']['change']:+.1%}"],
            ["Avg Transaction", f"${latest['avg_transaction_value']:,.2f}",
             f"{trends['avg_transaction_value']['change']:+.1%}"]
        ]
        
        # Create table
        table = Table(summary_data, colWidths=[200, 100, 100])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        content.append(table)
        return content
    
    def _create_metrics_section(self, metrics: pd.DataFrame) -> List:
        """Create metrics section."""
        content = []
        
        # Section title
        content.append(
            Paragraph("Detailed Metrics", self.styles['Section'])
        )
        content.append(Spacer(1, 12))
        
        # Create plots
        plt.figure(figsize=(10, 6))
        plt.plot(metrics['date'], metrics['total_revenue'])
        plt.title('Daily Revenue')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Save plot
        plot_path = '/tmp/revenue_plot.png'
        plt.savefig(plot_path)
        plt.close()
        
        # Add plot to PDF
        content.append(Image(plot_path, width=400, height=300))
        content.append(Spacer(1, 20))
        
        return content
    
    def _create_trends_section(self, trends: Dict[str, Any]) -> List:
        """Create trends section."""
        content = []
        
        # Section title
        content.append(
            Paragraph("Trend Analysis", self.styles['Section'])
        )
        content.append(Spacer(1, 12))
        
        # Trend summary
        trend_data = []
        for metric, data in trends.items():
            if isinstance(data, dict) and 'direction' in data:
                trend_data.append([
                    metric.replace('_', ' ').title(),
                    data['direction'].title(),
                    f"{data['strength']:.2%}",
                    "Yes" if data.get('is_seasonal') else "No"
                ])
        
        # Create table
        if trend_data:
            table = Table(
                [["Metric", "Trend", "Strength", "Seasonal"]] + trend_data,
                colWidths=[150, 100, 100, 100]
            )
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 12),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            content.append(table)
        
        return content 