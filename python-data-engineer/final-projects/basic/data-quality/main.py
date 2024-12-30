"""
Main entry point for data quality system.
"""

import click
from pathlib import Path
from datetime import datetime
import pandas as pd
from src.utils.config import Config
from src.utils.logging import setup_logging, get_logger
from src.validation.schema import SchemaValidator
from src.validation.checks import QualityChecker
from src.reporting.metrics import QualityMetrics
from src.reporting.dashboard import QualityDashboard
from src.alerts.notification import AlertNotifier
from src.alerts.threshold import ThresholdManager

# Set up logging
setup_logging()
logger = get_logger(__name__)

class QualitySystem:
    """Data quality system management."""
    
    def __init__(self):
        """Initialize system."""
        self.config = Config()
        self.validator = SchemaValidator(self.config)
        self.checker = QualityChecker(self.config)
        self.metrics = QualityMetrics(self.config)
        self.dashboard = QualityDashboard(self.config)
        self.notifier = AlertNotifier(self.config)
        self.threshold_manager = ThresholdManager(self.config)
    
    def run_quality_checks(
        self,
        dataset_name: str,
        save_metrics: bool = True
    ) -> dict:
        """
        Run quality checks for dataset.
        
        Args:
            dataset_name: Dataset name
            save_metrics: Whether to save metrics history
        
        Returns:
            Quality check results
        """
        try:
            logger.info(f"Running quality checks for {dataset_name}")
            
            # Load dataset
            dataset_config = self.config.datasets[dataset_name]
            df = pd.read_sql(
                dataset_config['source']['query'],
                dataset_config['source']['connection_string']
            )
            
            # Validate schema
            if not self.validator.validate_schema(df, dataset_name):
                self._handle_validation_failure(dataset_name)
                return None
            
            # Run quality checks
            check_results = self._run_checks(df, dataset_config['checks'])
            
            # Calculate metrics
            metrics = self.metrics.calculate_dataset_metrics(check_results)
            
            # Generate reports
            self._generate_reports(dataset_name, metrics, check_results)
            
            # Check thresholds and send alerts
            self._check_thresholds(dataset_name, metrics, check_results)
            
            # Save metrics history
            if save_metrics:
                self._save_metrics_history(dataset_name, metrics)
            
            logger.info(f"Completed quality checks for {dataset_name}")
            return {
                'metrics': metrics,
                'check_results': check_results
            }
            
        except Exception as e:
            logger.error(f"Quality checks failed: {e}")
            self.notifier.send_alert(
                'critical',
                f"Quality check system error for {dataset_name}",
                {'error': str(e)}
            )
            raise
    
    def _run_checks(self, df: pd.DataFrame, checks: list) -> list:
        """Run configured quality checks."""
        results = []
        for check in checks:
            check_type = check['type']
            if check_type == 'completeness':
                self.checker.check_completeness(
                    df,
                    check['columns'],
                    check['threshold']
                )
            elif check_type == 'uniqueness':
                self.checker.check_uniqueness(
                    df,
                    check['columns'],
                    check['threshold']
                )
            elif check_type == 'format':
                self.checker.check_format(
                    df,
                    check['column'],
                    check['pattern']
                )
            elif check_type == 'range':
                self.checker.check_range(
                    df,
                    check['column'],
                    check.get('min_value'),
                    check.get('max_value')
                )
        return self.checker.get_check_results()
    
    def _generate_reports(
        self,
        dataset_name: str,
        metrics: dict,
        check_results: list
    ) -> None:
        """Generate quality reports."""
        output_dir = Path("reports") / dataset_name / datetime.now().strftime('%Y%m%d')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate plots
        self.dashboard.create_quality_trend_plot(
            self._load_metrics_history(dataset_name),
            output_dir / "quality_trend.png"
        )
        
        self.dashboard.create_issues_summary_plot(
            check_results,
            output_dir / "issues_summary.png"
        )
        
        # Generate HTML report
        self.dashboard.generate_html_report(
            metrics,
            self.metrics.calculate_trend_metrics(
                self._load_metrics_history(dataset_name)
            ),
            check_results,
            output_dir / "report.html"
        )
    
    def _check_thresholds(
        self,
        dataset_name: str,
        metrics: dict,
        check_results: list
    ) -> None:
        """Check thresholds and send alerts."""
        quality_score = metrics['quality_score']
        alert_level = self.threshold_manager.check_threshold(
            'quality_score',
            quality_score
        )
        
        if alert_level:
            self.notifier.send_alert(
                alert_level,
                f"Quality score {quality_score:.2%} below threshold",
                {
                    'dataset': dataset_name,
                    'metrics': metrics,
                    'issues': [r for r in check_results if r['status'] == 'failed']
                }
            )
    
    def _save_metrics_history(
        self,
        dataset_name: str,
        metrics: dict
    ) -> None:
        """Save metrics to history."""
        history_dir = Path("metrics")
        history_dir.mkdir(exist_ok=True)
        
        history_file = history_dir / f"{dataset_name}.csv"
        
        # Load existing history
        if history_file.exists():
            history_df = pd.read_csv(history_file)
        else:
            history_df = pd.DataFrame()
        
        # Append new metrics
        new_df = pd.DataFrame([metrics])
        history_df = pd.concat([history_df, new_df], ignore_index=True)
        
        # Save with retention policy
        retention_days = self.config.metrics['history']['retention_days']
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=retention_days)
        history_df = history_df[
            pd.to_datetime(history_df['timestamp']) >= cutoff_date
        ]
        
        history_df.to_csv(history_file, index=False)
    
    def _load_metrics_history(self, dataset_name: str) -> list:
        """Load metrics history."""
        try:
            history_file = Path("metrics") / f"{dataset_name}.csv"
            if history_file.exists():
                return pd.read_csv(history_file).to_dict('records')
            return []
        except Exception as e:
            logger.error(f"Failed to load metrics history: {e}")
            return []

@click.group()
def cli():
    """Data Quality System CLI"""
    pass

@cli.command()
@click.option('--dataset', help='Dataset name')
@click.option('--save/--no-save', default=True, help='Save metrics history')
def check(dataset, save):
    """Run quality checks."""
    system = QualitySystem()
    if dataset:
        system.run_quality_checks(dataset, save)
    else:
        for dataset in system.config.datasets:
            system.run_quality_checks(dataset, save)

@cli.command()
@click.option('--dataset', required=True, help='Dataset name')
@click.option('--days', default=30, help='Number of days')
def history(dataset, days):
    """View metrics history."""
    system = QualitySystem()
    history = system.metrics.calculate_trend_metrics(
        system._load_metrics_history(dataset)
    )
    click.echo(history)

@cli.command()
@click.option('--port', default=8050, help='Dashboard port')
def dashboard(port):
    """Start quality dashboard."""
    # TODO: Implement web dashboard
    click.echo(f"Starting dashboard on port {port}")

if __name__ == "__main__":
    cli() 