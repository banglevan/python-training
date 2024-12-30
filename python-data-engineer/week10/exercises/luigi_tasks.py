"""
Luigi pipeline tasks exercise.
"""

import luigi
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import List, Dict
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataIngestionTask(luigi.Task):
    """Ingest data from source."""
    
    date = luigi.DateParameter(default=datetime.now())
    source_path = luigi.Parameter()
    
    def output(self):
        """Define output target."""
        return luigi.LocalTarget(
            f"data/raw/data_{self.date.strftime('%Y%m%d')}.parquet"
        )
    
    def run(self):
        """Execute task."""
        logger.info(f"Ingesting data from {self.source_path}")
        
        try:
            # Read data
            df = pd.read_csv(self.source_path)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
            
            # Save data
            df.to_parquet(self.output().path)
            logger.info(f"Data saved to {self.output().path}")
            
        except Exception as e:
            logger.error(f"Data ingestion failed: {e}")
            raise

class DataValidationTask(luigi.Task):
    """Validate data quality."""
    
    date = luigi.DateParameter(default=datetime.now())
    validation_rules = luigi.DictParameter()
    
    def requires(self):
        """Define dependencies."""
        return DataIngestionTask(date=self.date)
    
    def output(self):
        """Define output target."""
        return luigi.LocalTarget(
            f"data/validated/validation_{self.date.strftime('%Y%m%d')}.json"
        )
    
    def run(self):
        """Execute task."""
        logger.info("Validating data quality")
        
        try:
            # Read data
            df = pd.read_parquet(self.input().path)
            
            # Validate data
            validation_results = {
                'timestamp': datetime.now().isoformat(),
                'record_count': len(df),
                'checks': {}
            }
            
            for check_name, rule in self.validation_rules.items():
                if rule['type'] == 'null_check':
                    null_count = df[rule['column']].isnull().sum()
                    validation_results['checks'][check_name] = {
                        'null_count': int(null_count),
                        'passed': null_count <= rule.get('threshold', 0)
                    }
                
                elif rule['type'] == 'range_check':
                    in_range = df[rule['column']].between(
                        rule['min'],
                        rule['max']
                    ).all()
                    validation_results['checks'][check_name] = {
                        'passed': bool(in_range)
                    }
                
                elif rule['type'] == 'unique_check':
                    unique_count = df[rule['column']].nunique()
                    validation_results['checks'][check_name] = {
                        'unique_count': int(unique_count),
                        'passed': unique_count >= rule.get('min_unique', 1)
                    }
            
            # Save results
            os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
            with self.output().open('w') as f:
                json.dump(validation_results, f, indent=2)
            
            # Check if all validations passed
            all_passed = all(
                check['passed']
                for check in validation_results['checks'].values()
            )
            
            if not all_passed:
                raise ValueError("Data validation failed")
            
            logger.info("Data validation completed successfully")
            
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            raise

class DataTransformationTask(luigi.Task):
    """Transform data."""
    
    date = luigi.DateParameter(default=datetime.now())
    transformations = luigi.ListParameter()
    
    def requires(self):
        """Define dependencies."""
        return DataValidationTask(date=self.date)
    
    def output(self):
        """Define output target."""
        return luigi.LocalTarget(
            f"data/transformed/data_{self.date.strftime('%Y%m%d')}.parquet"
        )
    
    def run(self):
        """Execute task."""
        logger.info("Transforming data")
        
        try:
            # Read data
            input_path = DataIngestionTask(date=self.date).output().path
            df = pd.read_parquet(input_path)
            
            # Apply transformations
            for transform in self.transformations:
                if transform['type'] == 'rename':
                    df = df.rename(columns=transform['mapping'])
                
                elif transform['type'] == 'calculate':
                    df[transform['target']] = df.eval(transform['expression'])
                
                elif transform['type'] == 'filter':
                    df = df.query(transform['condition'])
                
                elif transform['type'] == 'aggregate':
                    df = df.groupby(transform['group_by']).agg(
                        transform['aggregations']
                    ).reset_index()
            
            # Save transformed data
            os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
            df.to_parquet(self.output().path)
            logger.info(f"Transformed data saved to {self.output().path}")
            
        except Exception as e:
            logger.error(f"Data transformation failed: {e}")
            raise

class DataExportTask(luigi.Task):
    """Export processed data."""
    
    date = luigi.DateParameter(default=datetime.now())
    export_format = luigi.Parameter(default='csv')
    export_options = luigi.DictParameter(default={})
    
    def requires(self):
        """Define dependencies."""
        return DataTransformationTask(date=self.date)
    
    def output(self):
        """Define output target."""
        return luigi.LocalTarget(
            f"data/exported/data_{self.date.strftime('%Y%m%d')}.{self.export_format}"
        )
    
    def run(self):
        """Execute task."""
        logger.info(f"Exporting data to {self.export_format}")
        
        try:
            # Read transformed data
            df = pd.read_parquet(self.input().path)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
            
            # Export based on format
            if self.export_format == 'csv':
                df.to_csv(self.output().path, **self.export_options)
            
            elif self.export_format == 'json':
                df.to_json(self.output().path, **self.export_options)
            
            elif self.export_format == 'excel':
                df.to_excel(self.output().path, **self.export_options)
            
            else:
                raise ValueError(f"Unsupported export format: {self.export_format}")
            
            logger.info(f"Data exported to {self.output().path}")
            
        except Exception as e:
            logger.error(f"Data export failed: {e}")
            raise

if __name__ == "__main__":
    # Example pipeline configuration
    validation_rules = {
        'id_check': {
            'type': 'unique_check',
            'column': 'id',
            'min_unique': 100
        },
        'value_check': {
            'type': 'range_check',
            'column': 'value',
            'min': 0,
            'max': 1000
        },
        'category_check': {
            'type': 'null_check',
            'column': 'category',
            'threshold': 0
        }
    }
    
    transformations = [
        {
            'type': 'rename',
            'mapping': {'old_name': 'new_name'}
        },
        {
            'type': 'calculate',
            'target': 'value_scaled',
            'expression': 'value * 100'
        },
        {
            'type': 'filter',
            'condition': 'value > 0'
        },
        {
            'type': 'aggregate',
            'group_by': ['category'],
            'aggregations': {
                'value': ['mean', 'sum', 'count']
            }
        }
    ]
    
    # Run pipeline
    luigi.build([
        DataExportTask(
            date=datetime.now(),
            export_format='csv',
            export_options={'index': False}
        )
    ], local_scheduler=True) 