"""
Data Quality Check
- Metrics calculation
- Data profiling
- Quality reports
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from dataclasses import dataclass
import json
import logging
from datetime import datetime
from scipy import stats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ColumnProfile:
    """Column profile metrics."""
    name: str
    dtype: str
    count: int
    unique_count: int
    missing_count: int
    missing_pct: float
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_value: Optional[float] = None
    most_common: List = None
    least_common: List = None

class DataQualityChecker:
    """Data quality checker class."""
    
    def __init__(self, df: pd.DataFrame):
        """Initialize checker."""
        self.df = df
        self.profiles = {}
        self.quality_scores = {}
    
    def profile_numeric(
        self,
        column: str
    ) -> ColumnProfile:
        """Profile numeric column."""
        series = self.df[column]
        
        return ColumnProfile(
            name=column,
            dtype='numeric',
            count=len(series),
            unique_count=series.nunique(),
            missing_count=series.isnull().sum(),
            missing_pct=series.isnull().mean() * 100,
            min_value=series.min(),
            max_value=series.max(),
            mean_value=series.mean(),
            median_value=series.median(),
            std_value=series.std(),
            most_common=series.value_counts()
                .head(5).to_dict(),
            least_common=series.value_counts()
                .tail(5).to_dict()
        )
    
    def profile_categorical(
        self,
        column: str
    ) -> ColumnProfile:
        """Profile categorical column."""
        series = self.df[column]
        
        return ColumnProfile(
            name=column,
            dtype='categorical',
            count=len(series),
            unique_count=series.nunique(),
            missing_count=series.isnull().sum(),
            missing_pct=series.isnull().mean() * 100,
            most_common=series.value_counts()
                .head(10).to_dict(),
            least_common=series.value_counts()
                .tail(5).to_dict()
        )
    
    def profile_datetime(
        self,
        column: str
    ) -> ColumnProfile:
        """Profile datetime column."""
        series = pd.to_datetime(
            self.df[column],
            errors='coerce'
        )
        
        return ColumnProfile(
            name=column,
            dtype='datetime',
            count=len(series),
            unique_count=series.nunique(),
            missing_count=series.isnull().sum(),
            missing_pct=series.isnull().mean() * 100,
            min_value=series.min(),
            max_value=series.max(),
            most_common=series.value_counts()
                .head(5).to_dict()
        )
    
    def profile_dataframe(self):
        """Profile entire dataframe."""
        for column in self.df.columns:
            # Detect data type
            if pd.api.types.is_numeric_dtype(
                self.df[column]
            ):
                self.profiles[column] = (
                    self.profile_numeric(column)
                )
            elif pd.api.types.is_datetime64_dtype(
                self.df[column]
            ):
                self.profiles[column] = (
                    self.profile_datetime(column)
                )
            else:
                self.profiles[column] = (
                    self.profile_categorical(column)
                )
    
    def calculate_quality_scores(self):
        """Calculate quality scores."""
        for column, profile in self.profiles.items():
            scores = {}
            
            # Completeness score
            scores['completeness'] = (
                1 - profile.missing_pct / 100
            )
            
            # Uniqueness score
            scores['uniqueness'] = (
                profile.unique_count / profile.count
            )
            
            # Validity score (based on data type)
            if profile.dtype == 'numeric':
                # Check for outliers using z-score
                z_scores = np.abs(
                    stats.zscore(
                        self.df[column].dropna()
                    )
                )
                scores['validity'] = (
                    (z_scores < 3).mean()
                )
            elif profile.dtype == 'datetime':
                # Check for valid dates
                valid_dates = pd.to_datetime(
                    self.df[column],
                    errors='coerce'
                ).notna()
                scores['validity'] = valid_dates.mean()
            else:
                # For categorical, check against allowed values
                # (if defined)
                scores['validity'] = 1.0
            
            # Overall quality score
            scores['overall'] = np.mean([
                scores['completeness'],
                scores['uniqueness'],
                scores['validity']
            ])
            
            self.quality_scores[column] = scores
    
    def generate_report(
        self,
        output_path: str
    ):
        """Generate quality report."""
        report = {
            "report_time": datetime.now().isoformat(),
            "dataset_info": {
                "rows": len(self.df),
                "columns": len(self.df.columns)
            },
            "column_profiles": {},
            "quality_scores": self.quality_scores
        }
        
        # Convert column profiles to dict
        for column, profile in self.profiles.items():
            report["column_profiles"][column] = (
                vars(profile)
            )
        
        # Save report
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Report saved to {output_path}")
    
    def print_summary(self):
        """Print quality summary."""
        logger.info("\nData Quality Summary:")
        logger.info("-" * 50)
        
        for column, scores in self.quality_scores.items():
            logger.info(f"\nColumn: {column}")
            logger.info(f"Type: {self.profiles[column].dtype}")
            logger.info(
                f"Completeness: {scores['completeness']:.2%}"
            )
            logger.info(
                f"Uniqueness: {scores['uniqueness']:.2%}"
            )
            logger.info(
                f"Validity: {scores['validity']:.2%}"
            )
            logger.info(
                f"Overall Quality: {scores['overall']:.2%}"
            )

def main():
    """Main function."""
    # Example data
    df = pd.DataFrame({
        "id": range(1000),
        "name": [f"User{i}" for i in range(1000)],
        "age": np.random.randint(18, 80, 1000),
        "salary": np.random.normal(50000, 10000, 1000),
        "department": np.random.choice(
            ['IT', 'HR', 'Sales', 'Marketing'],
            1000
        ),
        "join_date": pd.date_range(
            start='2020-01-01',
            periods=1000
        )
    })
    
    # Add some noise
    df.loc[np.random.choice(1000, 50), 'age'] = np.nan
    df.loc[np.random.choice(1000, 30), 'salary'] = -1
    df.loc[
        np.random.choice(1000, 20),
        'department'
    ] = None
    
    # Create checker
    checker = DataQualityChecker(df)
    
    # Run analysis
    checker.profile_dataframe()
    checker.calculate_quality_scores()
    
    # Generate report
    checker.generate_report('quality_report.json')
    
    # Print summary
    checker.print_summary()

if __name__ == "__main__":
    main() 