"""Core components for ETL system."""

from abc import ABC, abstractmethod
from typing import Dict, Any
from datetime import datetime
import logging
from .database import Database
from .metrics import MetricsManager

logger = logging.getLogger(__name__)

class Task(ABC):
    """Abstract base class for pipeline tasks."""
    
    def __init__(self, name: str, retries: int = 3):
        """Initialize task."""
        self.name = name
        self.retries = retries
        self.metrics = MetricsManager()
    
    @abstractmethod
    def execute(self, context: Dict) -> Dict:
        """Execute task."""
        pass
    
    def run(self, context: Dict) -> Dict:
        """Run task with retries and metrics."""
        for attempt in range(self.retries + 1):
            try:
                with self.metrics.task_duration(self.name):
                    result = self.execute(context)
                
                self.metrics.increment_task_success(self.name)
                return result
                
            except Exception as e:
                logger.error(f"Task {self.name} failed: {e}")
                self.metrics.increment_error(
                    context.get('pipeline_name'),
                    type(e).__name__
                )
                
                if attempt == self.retries:
                    raise

class Pipeline:
    """ETL pipeline implementation."""
    
    def __init__(self, name: str, db: Database, tasks: list):
        """Initialize pipeline."""
        self.name = name
        self.db = db
        self.tasks = tasks
        self.metrics = MetricsManager()
    
    def run(self, context: Dict) -> Dict:
        """Run pipeline."""
        start_time = datetime.now()
        run_id = self.db.insert_pipeline_run(self.name, 'RUNNING')
        context['pipeline_name'] = self.name
        
        try:
            # Execute tasks
            for task in self.tasks:
                task_result = task.run(context)
                context.update(task_result)
            
            # Calculate metrics
            duration = (datetime.now() - start_time).total_seconds()
            metrics = {
                'duration': duration,
                'records_processed': context.get('loaded_records', 0)
            }
            
            # Update pipeline status
            self.db.update_pipeline_run(run_id, 'SUCCESS', metrics)
            self.metrics.increment_pipeline_success(self.name)
            
            return context
            
        except Exception as e:
            # Handle failure
            self.db.update_pipeline_run(
                run_id,
                'FAILED',
                error_message=str(e)
            )
            self.metrics.increment_pipeline_failure(self.name)
            raise 