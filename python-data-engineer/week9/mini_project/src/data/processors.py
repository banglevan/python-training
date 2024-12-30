"""
Data processing module.
"""

import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor(ABC):
    """Abstract data processor base class."""
    
    @abstractmethod
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process data."""
        pass

class TimeSeriesProcessor(DataProcessor):
    """Time series data processor."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize processor."""
        self.config = config
        self.time_column = config.get('time_column', 'timestamp')
        self.value_column = config.get('value_column', 'value')
        self.aggregation = config.get('aggregation', 'mean')
        self.resample_rule = config.get('resample_rule', '1H')
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process time series data."""
        try:
            # Ensure datetime
            data[self.time_column] = pd.to_datetime(data[self.time_column])
            
            # Set index
            data = data.set_index(self.time_column)
            
            # Resample and aggregate
            result = data[self.value_column].resample(self.resample_rule)
            
            if self.aggregation == 'mean':
                result = result.mean()
            elif self.aggregation == 'sum':
                result = result.sum()
            elif self.aggregation == 'count':
                result = result.count()
            else:
                raise ValueError(f"Unsupported aggregation: {self.aggregation}")
            
            return result.reset_index()
            
        except Exception as e:
            logger.error(f"Time series processing failed: {e}")
            raise

class AggregationProcessor(DataProcessor):
    """Data aggregation processor."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize processor."""
        self.config = config
        self.group_by = config.get('group_by', [])
        self.metrics = config.get('metrics', {})
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process aggregation."""
        try:
            if not self.group_by:
                return data
            
            agg_dict = {}
            for col, metric in self.metrics.items():
                if metric == 'sum':
                    agg_dict[col] = 'sum'
                elif metric == 'mean':
                    agg_dict[col] = 'mean'
                elif metric == 'count':
                    agg_dict[col] = 'count'
                elif metric == 'unique':
                    agg_dict[col] = 'nunique'
            
            return data.groupby(self.group_by).agg(agg_dict).reset_index()
            
        except Exception as e:
            logger.error(f"Aggregation processing failed: {e}")
            raise

class FilterProcessor(DataProcessor):
    """Data filtering processor."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize processor."""
        self.config = config
        self.conditions = config.get('conditions', [])
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process filters."""
        try:
            for condition in self.conditions:
                column = condition['column']
                operator = condition['operator']
                value = condition['value']
                
                if operator == 'eq':
                    data = data[data[column] == value]
                elif operator == 'gt':
                    data = data[data[column] > value]
                elif operator == 'lt':
                    data = data[data[column] < value]
                elif operator == 'in':
                    data = data[data[column].isin(value)]
                elif operator == 'between':
                    data = data[
                        (data[column] >= value[0]) &
                        (data[column] <= value[1])
                    ]
            
            return data
            
        except Exception as e:
            logger.error(f"Filter processing failed: {e}")
            raise

class ProcessorPipeline:
    """Data processor pipeline."""
    
    def __init__(self, processors: List[DataProcessor]):
        """Initialize pipeline."""
        self.processors = processors
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Execute processing pipeline."""
        try:
            result = data.copy()
            for processor in self.processors:
                result = processor.process(result)
            return result
            
        except Exception as e:
            logger.error(f"Pipeline processing failed: {e}")
            raise

class ProcessorFactory:
    """Data processor factory."""
    
    @staticmethod
    def create_processor(processor_config: Dict[str, Any]) -> DataProcessor:
        """Create processor instance."""
        processor_type = processor_config['type'].lower()
        
        if processor_type == 'timeseries':
            return TimeSeriesProcessor(processor_config)
        elif processor_type == 'aggregation':
            return AggregationProcessor(processor_config)
        elif processor_type == 'filter':
            return FilterProcessor(processor_config)
        else:
            raise ValueError(f"Unsupported processor type: {processor_type}") 