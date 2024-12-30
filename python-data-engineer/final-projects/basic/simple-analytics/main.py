"""
Main entry point for analytics platform.
"""

import click
from pathlib import Path
import logging
from datetime import datetime, timedelta
from src.utils.config import Config
from src.utils.logging import setup_logging
from src.utils.database import Database

# Import ingestion components
from src.ingestion.connectors.database import DatabaseConnector
from src.ingestion.connectors.csv import CSVConnector
from src.ingestion.validators.schema import SchemaValidator
from src.ingestion.loaders.warehouse import WarehouseLoader

# Import analytics components
from src.analytics.processors.metrics import MetricsProcessor
from src.analytics.processors.trends import TrendDetector
from src.analytics.calculators.metrics import MetricsCalculator
from src.analytics.aggregators.time import TimeAggregator
from src.analytics.aggregators.dimension import DimensionAggregator

# Import reporting components
from src.reporting.templates.html import HTMLTemplate
from src.reporting.distributors.email import EmailDistributor
from src.reporting.distributors.slack import SlackDistributor

# Import API
from src.dashboard.backend.api import app
import uvicorn

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

@click.group()
def cli():
    """Analytics Platform CLI"""
    pass

@cli.command()
@click.option('--source', required=True, help='Data source name')
@click.option('--start-date', type=click.DateTime(), help='Start date')
@click.option('--connector-type', type=click.Choice(['database', 'csv']), default='database')
def ingest(source, start_date, connector_type):
    """Ingest data from source."""
    try:
        config = Config()
        db = Database(config)
        
        # Initialize components
        connector = (
            DatabaseConnector(config)
            if connector_type == 'database'
            else CSVConnector(config)
        )
        validator = SchemaValidator(config)
        loader = WarehouseLoader(config)
        
        # Load data
        data = connector.read(start_date=start_date)
        
        # Validate
        if validator.validate(data, source):
            # Load to warehouse
            loader.load_fact(data, f"fact_{source}", ['id'])
            logger.info(f"Ingested data from {source}")
        else:
            logger.error(f"Validation failed for {source}")
            for error in validator.get_errors():
                logger.error(f"Validation error: {error}")
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise
    finally:
        if connector:
            connector.close()

@cli.command()
@click.option('--type', required=True, help='Report type')
@click.option('--start-date', type=click.DateTime(), required=True, help='Start date')
@click.option('--end-date', type=click.DateTime(), required=True, help='End date')
@click.option('--format', type=click.Choice(['html', 'pdf']), default='html')
@click.option('--email', multiple=True, help='Email recipients')
@click.option('--slack-channel', help='Slack channel')
def report(type, start_date, end_date, format, email, slack_channel):
    """Generate and distribute report."""
    try:
        config = Config()
        db = Database(config)
        
        # Initialize analytics components
        metrics_processor = MetricsProcessor(config)
        trend_detector = TrendDetector(config)
        metrics_calculator = MetricsCalculator(config)
        time_aggregator = TimeAggregator(config)
        dimension_aggregator = DimensionAggregator(config)
        
        # Initialize reporting components
        template = HTMLTemplate(config)
        
        # Process data
        with db.session() as session:
            # Get metrics
            sales_data = metrics_processor.process_sales_metrics(
                session, start_date, end_date
            )
            trends = trend_detector.detect_trends(
                sales_data, 'total_revenue'
            )
            
            # Add report sections
            template.add_section(
                "Executive Summary",
                f"Report period: {start_date.date()} to {end_date.date()}"
            )
            
            template.add_chart(
                "Revenue Trend",
                sales_data,
                'line',
                x='date',
                y='total_revenue'
            )
            
            # Add aggregated metrics
            daily_metrics = time_aggregator.aggregate_daily(
                sales_data,
                'date',
                [{'name': 'revenue', 'type': 'sum'}]
            )
            template.add_table("Daily Metrics", daily_metrics)
        
        # Generate report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f"reports/{type}_{timestamp}.html"
        template.render(output_path)
        
        # Distribute report
        if email:
            email_distributor = EmailDistributor(config)
            email_distributor.send_report(
                recipients=list(email),
                subject=f"Analytics Report - {type}",
                body="Please find attached the analytics report.",
                attachments=[output_path]
            )
        
        if slack_channel:
            slack_distributor = SlackDistributor(config)
            slack_distributor.send_report(
                channel=slack_channel,
                message=f"Analytics Report - {type}",
                attachments=[output_path]
            )
        
        logger.info(f"Generated and distributed {type} report")
        
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        raise

@cli.command()
@click.option('--port', default=8000, help='Server port')
@click.option('--host', default='127.0.0.1', help='Server host')
@click.option('--reload', is_flag=True, help='Enable auto-reload')
def server(port, host, reload):
    """Start API server."""
    try:
        logger.info(f"Starting server on {host}:{port}")
        uvicorn.run(
            "src.dashboard.backend.api:app",
            host=host,
            port=port,
            reload=reload
        )
    except Exception as e:
        logger.error(f"Server failed: {e}")
        raise

@cli.command()
def init_db():
    """Initialize database schema."""
    try:
        config = Config()
        db = Database(config)
        
        with db.session() as session:
            # Create tables
            session.execute("""
                CREATE TABLE IF NOT EXISTS dim_customers (
                    customer_id INTEGER PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    segment VARCHAR(50),
                    created_at TIMESTAMP
                )
            """)
            
            session.execute("""
                CREATE TABLE IF NOT EXISTS dim_products (
                    product_id INTEGER PRIMARY KEY,
                    name VARCHAR(100),
                    category VARCHAR(50),
                    subcategory VARCHAR(50),
                    price DECIMAL(10,2),
                    created_at TIMESTAMP
                )
            """)
            
            session.execute("""
                CREATE TABLE IF NOT EXISTS fact_sales (
                    sale_id INTEGER PRIMARY KEY,
                    customer_id INTEGER REFERENCES dim_customers(customer_id),
                    product_id INTEGER REFERENCES dim_products(product_id),
                    sale_date DATE,
                    quantity INTEGER,
                    unit_price DECIMAL(10,2),
                    total_amount DECIMAL(10,2),
                    created_at TIMESTAMP
                )
            """)
            
        logger.info("Initialized database schema")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

if __name__ == "__main__":
    cli() 