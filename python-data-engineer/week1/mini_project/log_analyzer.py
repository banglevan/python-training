"""
Log Analysis Pipeline
- Parse log files
- Extract metrics
- Generate reports
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dataclasses import dataclass
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class LogEntry:
    """Log entry structure."""
    timestamp: datetime
    level: str
    service: str
    message: str
    error_code: Optional[str] = None
    user_id: Optional[str] = None
    duration_ms: Optional[float] = None

class LogAnalyzer:
    """Log analysis pipeline."""
    
    def __init__(self):
        """Initialize analyzer."""
        self.log_entries: List[LogEntry] = []
        self.metrics: Dict = {}
    
    def parse_log_line(
        self,
        line: str
    ) -> Optional[LogEntry]:
        """Parse single log line."""
        try:
            # Example log format:
            # 2024-01-15 10:30:45 INFO [UserService] 
            # User login successful - user_id=123 
            # duration_ms=150
            pattern = (
                r"(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})"
                r"\s+(\w+)\s+\[(\w+)\]\s+(.*)"
            )
            
            match = re.match(pattern, line)
            if not match:
                return None
            
            timestamp_str, level, service, message = (
                match.groups()
            )
            
            # Parse timestamp
            timestamp = datetime.strptime(
                timestamp_str,
                "%Y-%m-%d %H:%M:%S"
            )
            
            # Extract additional fields
            error_code = None
            user_id = None
            duration_ms = None
            
            # Extract error code
            error_match = re.search(
                r"error_code=(\w+)",
                message
            )
            if error_match:
                error_code = error_match.group(1)
            
            # Extract user ID
            user_match = re.search(
                r"user_id=(\d+)",
                message
            )
            if user_match:
                user_id = user_match.group(1)
            
            # Extract duration
            duration_match = re.search(
                r"duration_ms=(\d+)",
                message
            )
            if duration_match:
                duration_ms = float(
                    duration_match.group(1)
                )
            
            return LogEntry(
                timestamp=timestamp,
                level=level,
                service=service,
                message=message,
                error_code=error_code,
                user_id=user_id,
                duration_ms=duration_ms
            )
            
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None
    
    def parse_log_file(self, file_path: str):
        """Parse entire log file."""
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    entry = self.parse_log_line(line.strip())
                    if entry:
                        self.log_entries.append(entry)
            
            logger.info(
                f"Parsed {len(self.log_entries)} log entries"
            )
            
        except Exception as e:
            logger.error(f"File parsing error: {e}")
            raise
    
    def calculate_metrics(self):
        """Calculate log metrics."""
        try:
            # Convert to DataFrame for easier analysis
            df = pd.DataFrame([
                vars(entry) 
                for entry in self.log_entries
            ])
            
            # Basic metrics
            self.metrics['total_entries'] = len(df)
            self.metrics['date_range'] = {
                'start': df['timestamp'].min(),
                'end': df['timestamp'].max()
            }
            
            # Error analysis
            error_df = df[df['level'] == 'ERROR']
            self.metrics['error_count'] = len(error_df)
            self.metrics['error_rate'] = (
                len(error_df) / len(df)
            )
            
            if not error_df.empty:
                self.metrics['top_errors'] = (
                    error_df['error_code']
                    .value_counts()
                    .head(5)
                    .to_dict()
                )
            
            # Service analysis
            self.metrics['service_stats'] = (
                df.groupby('service')
                .agg({
                    'timestamp': 'count',
                    'duration_ms': ['mean', 'median', 'std']
                })
                .to_dict()
            )
            
            # Time analysis
            df['hour'] = df['timestamp'].dt.hour
            self.metrics['hourly_stats'] = (
                df.groupby('hour')
                .size()
                .to_dict()
            )
            
            logger.info("Metrics calculated successfully")
            
        except Exception as e:
            logger.error(f"Metrics calculation error: {e}")
            raise
    
    def generate_visualizations(
        self,
        output_dir: str
    ):
        """Generate visualization plots."""
        try:
            Path(output_dir).mkdir(
                parents=True,
                exist_ok=True
            )
            
            # Convert to DataFrame
            df = pd.DataFrame([
                vars(entry) 
                for entry in self.log_entries
            ])
            
            # 1. Error distribution by service
            plt.figure(figsize=(10, 6))
            error_by_service = (
                df[df['level'] == 'ERROR']
                .groupby('service')
                .size()
            )
            error_by_service.plot(kind='bar')
            plt.title('Errors by Service')
            plt.tight_layout()
            plt.savefig(
                f"{output_dir}/errors_by_service.png"
            )
            
            # 2. Request duration distribution
            plt.figure(figsize=(10, 6))
            sns.histplot(
                data=df,
                x='duration_ms',
                bins=50
            )
            plt.title('Request Duration Distribution')
            plt.tight_layout()
            plt.savefig(
                f"{output_dir}/duration_dist.png"
            )
            
            # 3. Activity heatmap
            plt.figure(figsize=(12, 6))
            df['hour'] = df['timestamp'].dt.hour
            df['day'] = df['timestamp'].dt.day_name()
            activity = pd.crosstab(
                df['day'],
                df['hour']
            )
            sns.heatmap(
                activity,
                cmap='YlOrRd',
                annot=True,
                fmt='d'
            )
            plt.title('Activity Heatmap')
            plt.tight_layout()
            plt.savefig(
                f"{output_dir}/activity_heatmap.png"
            )
            
            logger.info(
                f"Visualizations saved to {output_dir}"
            )
            
        except Exception as e:
            logger.error(
                f"Visualization generation error: {e}"
            )
            raise
    
    def generate_report(
        self,
        output_file: str
    ):
        """Generate analysis report."""
        try:
            report = {
                "analysis_time": (
                    datetime.now().isoformat()
                ),
                "metrics": self.metrics,
                "summary": {
                    "total_logs": (
                        self.metrics['total_entries']
                    ),
                    "error_rate": (
                        f"{self.metrics['error_rate']:.2%}"
                    ),
                    "date_range": {
                        "start": self.metrics['date_range']
                            ['start'].isoformat(),
                        "end": self.metrics['date_range']
                            ['end'].isoformat()
                    }
                },
                "recommendations": []
            }
            
            # Add recommendations based on metrics
            if self.metrics['error_rate'] > 0.05:
                report["recommendations"].append(
                    "High error rate detected. "
                    "Review error patterns."
                )
            
            # Check service performance
            for service, stats in (
                self.metrics['service_stats'].items()
            ):
                if stats['duration_ms']['mean'] > 1000:
                    report["recommendations"].append(
                        f"High latency in {service}. "
                        "Consider optimization."
                    )
            
            # Save report
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"Report saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Report generation error: {e}")
            raise

def main():
    """Main function."""
    # Initialize analyzer
    analyzer = LogAnalyzer()
    
    # Parse logs
    analyzer.parse_log_file("logs/application.log")
    
    # Calculate metrics
    analyzer.calculate_metrics()
    
    # Generate visualizations
    analyzer.generate_visualizations("reports/plots")
    
    # Generate report
    analyzer.generate_report("reports/analysis.json")

if __name__ == "__main__":
    main() 