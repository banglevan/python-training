"""
Ray processing exercise.

This module demonstrates advanced Ray features including:
1. Actor Pattern
2. Distributed Training
3. Batch Processing
4. Parameter Tuning
5. Resource Management
"""

import ray
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import logging
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import time
import json
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@ray.remote(num_cpus=1, max_retries=3)
class DataProcessor:
    """
    Ray actor for data processing with state management.
    
    Features:
    - Batch processing
    - Data caching
    - State tracking
    - Performance monitoring
    """
    
    def __init__(self, processor_id: int, cache_size: int = 1000):
        """
        Initialize processor.
        
        Args:
            processor_id: Unique identifier for the processor
            cache_size: Maximum number of items to cache
        """
        self.processor_id = processor_id
        self.max_cache_size = cache_size
        self.data_cache = {}
        self.start_time = datetime.now()
        self.metrics = {
            'processed_batches': 0,
            'total_items': 0,
            'errors': 0,
            'processing_time': 0
        }
    
    def process_batch(
        self,
        data: np.ndarray,
        operation: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        """
        Process data batch with performance tracking.
        
        Args:
            data: Input data array
            operation: Processing operation type
            metadata: Additional processing metadata
        
        Returns:
            Tuple of (processed_data, processing_metrics)
        """
        start_time = time.time()
        
        try:
            # Basic data validation
            if data.size == 0:
                raise ValueError("Empty batch received")
            
            # Process based on operation type
            if operation == 'normalize':
                result = (data - data.mean()) / data.std()
                
            elif operation == 'standardize':
                result = (data - data.min()) / (data.max() - data.min())
                
            elif operation == 'log':
                result = np.log1p(data)
                
            elif operation == 'moving_average':
                window = metadata.get('window', 3)
                result = np.convolve(data, np.ones(window)/window, mode='valid')
                
            elif operation == 'outlier_removal':
                zscore = np.abs((data - data.mean()) / data.std())
                threshold = metadata.get('threshold', 3)
                result = data[zscore < threshold]
                
            else:
                raise ValueError(f"Unsupported operation: {operation}")
            
            # Update cache with result hash
            cache_key = hash(str(data) + operation)
            if len(self.data_cache) >= self.max_cache_size:
                # Remove oldest entry if cache is full
                self.data_cache.pop(next(iter(self.data_cache)))
            self.data_cache[cache_key] = result
            
            # Update metrics
            processing_time = time.time() - start_time
            self.metrics['processed_batches'] += 1
            self.metrics['total_items'] += data.size
            self.metrics['processing_time'] += processing_time
            
            batch_metrics = {
                'batch_size': data.size,
                'processing_time': processing_time,
                'operation': operation,
                'success': True
            }
            
            return result, batch_metrics
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            self.metrics['errors'] += 1
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get detailed processor statistics.
        """
        uptime = (datetime.now() - self.start_time).seconds
        avg_processing_time = (
            self.metrics['processing_time'] / 
            max(self.metrics['processed_batches'], 1)
        )
        
        return {
            'processor_id': self.processor_id,
            'uptime_seconds': uptime,
            'cache_usage': f"{len(self.data_cache)}/{self.max_cache_size}",
            'metrics': {
                **self.metrics,
                'average_batch_time': avg_processing_time,
                'items_per_second': self.metrics['total_items'] / max(uptime, 1)
            }
        }

@ray.remote(num_cpus=2, max_retries=2)
class ModelTrainer:
    """
    Distributed model trainer with hyperparameter tuning.
    """
    
    def __init__(self, model_dir: str = "models"):
        """
        Initialize trainer.
        
        Args:
            model_dir: Directory to save trained models
        """
        self.model_dir = model_dir
        os.makedirs(model_dir, exist_ok=True)
        self.training_history = []
    
    def train(
        self,
        X: np.ndarray,
        y: np.ndarray,
        model_config: Dict[str, Any],
        validation_split: float = 0.2
    ) -> Dict[str, Any]:
        """
        Train and evaluate model.
        
        Args:
            X: Feature matrix
            y: Target vector
            model_config: Model configuration
            validation_split: Validation data fraction
        
        Returns:
            Training results and metrics
        """
        try:
            # Split data
            X_train, X_val, y_train, y_val = train_test_split(
                X, y,
                test_size=validation_split,
                random_state=42
            )
            
            # Initialize model
            model_type = model_config['type']
            params = model_config.get('params', {})
            
            if model_type == 'linear':
                from sklearn.linear_model import LinearRegression
                model = LinearRegression(**params)
                
            elif model_type == 'rf':
                from sklearn.ensemble import RandomForestRegressor
                model = RandomForestRegressor(**params)
                
            elif model_type == 'xgb':
                import xgboost as xgb
                model = xgb.XGBRegressor(**params)
                
            else:
                raise ValueError(f"Unsupported model: {model_type}")
            
            # Train and evaluate
            start_time = time.time()
            model.fit(X_train, y_train)
            training_time = time.time() - start_time
            
            # Calculate metrics
            train_score = model.score(X_train, y_train)
            val_score = model.score(X_val, y_val)
            val_pred = model.predict(X_val)
            val_mse = mean_squared_error(y_val, val_pred)
            val_r2 = r2_score(y_val, val_pred)
            
            # Save model and results
            model_path = os.path.join(
                self.model_dir,
                f"{model_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
            )
            
            import joblib
            joblib.dump(model, model_path)
            
            results = {
                'model_type': model_type,
                'params': params,
                'metrics': {
                    'train_score': train_score,
                    'val_score': val_score,
                    'val_mse': val_mse,
                    'val_r2': val_r2,
                    'training_time': training_time
                },
                'model_path': model_path,
                'data_shape': {
                    'X_train': X_train.shape,
                    'X_val': X_val.shape
                }
            }
            
            self.training_history.append(results)
            return results
            
        except Exception as e:
            logger.error(f"Model training failed: {e}")
            raise

@ray.remote
def parameter_search(
    X: np.ndarray,
    y: np.ndarray,
    model_type: str,
    param_grid: Dict[str, List[Any]]
) -> List[Dict[str, Any]]:
    """
    Distributed hyperparameter search.
    
    Args:
        X: Feature matrix
        y: Target vector
        model_type: Type of model to train
        param_grid: Grid of parameters to search
    
    Returns:
        List of training results for each parameter combination
    """
    from itertools import product
    
    # Generate parameter combinations
    param_names = list(param_grid.keys())
    param_values = list(param_grid.values())
    param_combinations = list(product(*param_values))
    
    results = []
    trainer = ModelTrainer.remote()
    
    # Train models in parallel
    futures = []
    for params in param_combinations:
        model_config = {
            'type': model_type,
            'params': dict(zip(param_names, params))
        }
        futures.append(trainer.train.remote(X, y, model_config))
    
    # Collect results
    results = ray.get(futures)
    return results

class RayProcessor:
    """
    Ray distributed processing manager with advanced features.
    """
    
    def __init__(
        self,
        num_processors: int = 4,
        cache_size: int = 1000,
        init_kwargs: Optional[Dict[str, Any]] = None
    ):
        """Initialize Ray cluster with configuration."""
        ray.init(**(init_kwargs or {}))
        
        # Initialize processors with different cache sizes
        self.processors = [
            DataProcessor.remote(
                i,
                cache_size=cache_size // num_processors
            )
            for i in range(num_processors)
        ]
        
        logger.info(
            f"Initialized Ray with {num_processors} processors "
            f"and {cache_size} total cache size"
        )
    
    def parallel_processing(
        self,
        data: np.ndarray,
        operation: str,
        batch_size: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Tuple[List[np.ndarray], Dict[str, Any]]:
        """
        Process data in parallel with detailed metrics.
        """
        try:
            # Split data into batches
            batches = np.array_split(data, batch_size)
            
            # Process batches in parallel
            futures = [
                processor.process_batch.remote(
                    batch,
                    operation,
                    metadata
                )
                for processor, batch in zip(
                    self.processors,
                    batches
                )
            ]
            
            # Get results and metrics
            results = ray.get(futures)
            processed_data = [r[0] for r in results]
            batch_metrics = [r[1] for r in results]
            
            # Aggregate metrics
            total_time = sum(m['processing_time'] for m in batch_metrics)
            total_items = sum(m['batch_size'] for m in batch_metrics)
            
            metrics = {
                'operation': operation,
                'num_batches': len(batches),
                'total_items': total_items,
                'total_time': total_time,
                'items_per_second': total_items / total_time if total_time > 0 else 0,
                'batch_metrics': batch_metrics
            }
            
            logger.info(f"Parallel processing completed: {metrics}")
            return processed_data, metrics
            
        except Exception as e:
            logger.error(f"Parallel processing failed: {e}")
            raise
    
    def distributed_training(
        self,
        data: List[Dict[str, np.ndarray]],
        models: List[Dict[str, Any]],
        param_grids: Optional[Dict[str, Dict[str, List[Any]]]] = None
    ) -> Dict[str, Any]:
        """
        Distributed model training with optional parameter search.
        """
        try:
            results = []
            
            if param_grids:
                # Perform parameter search for each model type
                for model_type, param_grid in param_grids.items():
                    search_results = parameter_search.remote(
                        data[0]['X'],
                        data[0]['y'],
                        model_type,
                        param_grid
                    )
                    results.extend(ray.get(search_results))
            else:
                # Train specified models
                trainers = [ModelTrainer.remote() for _ in range(len(models))]
                futures = [
                    trainer.train.remote(
                        batch['X'],
                        batch['y'],
                        model
                    )
                    for trainer, batch, model in zip(trainers, data, models)
                ]
                results = ray.get(futures)
            
            # Aggregate results
            summary = {
                'num_models': len(results),
                'best_model': max(
                    results,
                    key=lambda x: x['metrics']['val_score']
                ),
                'training_time': sum(
                    r['metrics']['training_time']
                    for r in results
                ),
                'all_results': results
            }
            
            logger.info(f"Distributed training completed: {len(results)} models")
            return summary
            
        except Exception as e:
            logger.error(f"Distributed training failed: {e}")
            raise
    
    def get_processor_stats(self) -> Dict[str, Any]:
        """
        Get detailed statistics from all processors.
        """
        try:
            stats = ray.get([p.get_stats.remote() for p in self.processors])
            
            # Aggregate stats
            total_processed = sum(s['metrics']['processed_batches'] for s in stats)
            total_errors = sum(s['metrics']['errors'] for s in stats)
            total_items = sum(s['metrics']['total_items'] for s in stats)
            total_time = sum(s['metrics']['processing_time'] for s in stats)
            
            return {
                'processor_stats': stats,
                'aggregate_metrics': {
                    'total_processed_batches': total_processed,
                    'total_errors': total_errors,
                    'total_items': total_items,
                    'total_processing_time': total_time,
                    'average_processing_time': total_time / max(total_processed, 1),
                    'error_rate': total_errors / max(total_processed, 1)
                }
            }
            
        except Exception as e:
            logger.error(f"Stats retrieval failed: {e}")
            raise
    
    def cleanup(self):
        """
        Cleanup Ray cluster and save final stats.
        """
        try:
            # Get final stats
            final_stats = self.get_processor_stats()
            
            # Save stats to file
            stats_file = f"ray_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(stats_file, 'w') as f:
                json.dump(final_stats, f, indent=2)
            
            # Shutdown cluster
            ray.shutdown()
            logger.info(f"Ray cluster cleaned up. Stats saved to {stats_file}")
            
        except Exception as e:
            logger.error(f"Cluster cleanup failed: {e}")
            raise

if __name__ == "__main__":
    # Example usage with advanced features
    processor = RayProcessor(num_processors=4, cache_size=2000)
    
    try:
        # Generate sample data
        X = np.random.randn(10000, 20)
        y = np.random.randn(10000)
        
        # Parallel processing with different operations
        operations = ['normalize', 'standardize', 'moving_average']
        all_results = {}
        
        for op in operations:
            results, metrics = processor.parallel_processing(
                data=X,
                operation=op,
                batch_size=4,
                metadata={'window': 3}
            )
            all_results[op] = metrics
        
        print("Processing results:", all_results)
        
        # Distributed training with parameter search
        param_grids = {
            'rf': {
                'n_estimators': [100, 200],
                'max_depth': [10, 20]
            },
            'xgb': {
                'n_estimators': [100, 200],
                'max_depth': [3, 5]
            }
        }
        
        training_results = processor.distributed_training(
            data=[{'X': X, 'y': y}],
            models=[],  # Empty for parameter search
            param_grids=param_grids
        )
        
        print("Best model:", training_results['best_model'])
        
        # Get detailed stats
        stats = processor.get_processor_stats()
        print("Cluster stats:", stats['aggregate_metrics'])
        
    finally:
        processor.cleanup() 