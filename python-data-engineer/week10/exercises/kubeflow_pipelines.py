"""
Kubeflow pipelines exercise.
"""

import kfp
from kfp import dsl
from kfp.components import create_component_from_func
from typing import NamedTuple
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@create_component_from_func
def load_data(
    source_path: str
) -> NamedTuple('Outputs', [
    ('data_path', str),
    ('stats', dict)
]):
    """Load and prepare data."""
    import pandas as pd
    from datetime import datetime
    
    # Load data
    df = pd.read_csv(source_path)
    
    # Calculate stats
    stats = {
        'timestamp': datetime.now().isoformat(),
        'rows': len(df),
        'columns': len(df.columns),
        'missing_values': df.isnull().sum().to_dict()
    }
    
    # Save data
    output_path = '/tmp/data.parquet'
    df.to_parquet(output_path)
    
    from collections import namedtuple
    output = namedtuple('Outputs', ['data_path', 'stats'])
    return output(output_path, stats)

@create_component_from_func
def preprocess_data(
    data_path: str,
    target_column: str,
    test_size: float = 0.2
) -> NamedTuple('Outputs', [
    ('train_path', str),
    ('test_path', str),
    ('preprocessing_stats', dict)
]):
    """Preprocess and split data."""
    import pandas as pd
    from sklearn.model_selection import train_test_split
    
    # Load data
    df = pd.read_parquet(data_path)
    
    # Split features and target
    X = df.drop(target_column, axis=1)
    y = df[target_column]
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=test_size,
        random_state=42
    )
    
    # Save splits
    train_path = '/tmp/train.parquet'
    test_path = '/tmp/test.parquet'
    
    pd.concat([X_train, y_train], axis=1).to_parquet(train_path)
    pd.concat([X_test, y_test], axis=1).to_parquet(test_path)
    
    # Calculate stats
    stats = {
        'train_size': len(X_train),
        'test_size': len(X_test),
        'features': list(X.columns)
    }
    
    from collections import namedtuple
    output = namedtuple('Outputs', ['train_path', 'test_path', 'preprocessing_stats'])
    return output(train_path, test_path, stats)

@create_component_from_func
def train_model(
    train_path: str,
    target_column: str,
    model_params: dict
) -> NamedTuple('Outputs', [
    ('model_path', str),
    ('training_stats', dict)
]):
    """Train machine learning model."""
    import pandas as pd
    import pickle
    from sklearn.ensemble import RandomForestRegressor
    from datetime import datetime
    
    # Load data
    df = pd.read_parquet(train_path)
    X = df.drop(target_column, axis=1)
    y = df[target_column]
    
    # Train model
    model = RandomForestRegressor(**model_params)
    model.fit(X, y)
    
    # Save model
    model_path = '/tmp/model.pkl'
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    
    # Calculate stats
    stats = {
        'timestamp': datetime.now().isoformat(),
        'feature_importance': dict(zip(
            X.columns,
            model.feature_importances_
        )),
        'params': model_params
    }
    
    from collections import namedtuple
    output = namedtuple('Outputs', ['model_path', 'training_stats'])
    return output(model_path, stats)

@create_component_from_func
def evaluate_model(
    model_path: str,
    test_path: str,
    target_column: str
) -> NamedTuple('Outputs', [
    ('metrics_path', str),
    ('evaluation_stats', dict)
]):
    """Evaluate model performance."""
    import pandas as pd
    import pickle
    from sklearn.metrics import mean_squared_error, r2_score
    import json
    
    # Load model and data
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    df = pd.read_parquet(test_path)
    X = df.drop(target_column, axis=1)
    y = df[target_column]
    
    # Make predictions
    y_pred = model.predict(X)
    
    # Calculate metrics
    metrics = {
        'mse': mean_squared_error(y, y_pred),
        'rmse': mean_squared_error(y, y_pred, squared=False),
        'r2': r2_score(y, y_pred)
    }
    
    # Save metrics
    metrics_path = '/tmp/metrics.json'
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f)
    
    from collections import namedtuple
    output = namedtuple('Outputs', ['metrics_path', 'evaluation_stats'])
    return output(metrics_path, metrics)

@dsl.pipeline(
    name='ML Training Pipeline',
    description='End-to-end ML training pipeline'
)
def ml_pipeline(
    source_path: str,
    target_column: str,
    test_size: float = 0.2,
    model_params: dict = {'n_estimators': 100}
):
    """Define ML pipeline."""
    
    # Load data
    load_task = load_data(source_path)
    
    # Preprocess data
    preprocess_task = preprocess_data(
        data_path=load_task.outputs['data_path'],
        target_column=target_column,
        test_size=test_size
    )
    
    # Train model
    train_task = train_model(
        train_path=preprocess_task.outputs['train_path'],
        target_column=target_column,
        model_params=model_params
    )
    
    # Evaluate model
    evaluate_task = evaluate_model(
        model_path=train_task.outputs['model_path'],
        test_path=preprocess_task.outputs['test_path'],
        target_column=target_column
    )

if __name__ == "__main__":
    # Compile pipeline
    kfp.compiler.Compiler().compile(
        ml_pipeline,
        'ml_pipeline.yaml'
    )
    
    # Optional: Run pipeline
    client = kfp.Client()
    run = client.create_run_from_pipeline_func(
        ml_pipeline,
        arguments={
            'source_path': 'data/input.csv',
            'target_column': 'target',
            'test_size': 0.2,
            'model_params': {'n_estimators': 100}
        }
    ) 