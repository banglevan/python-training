# ETL Orchestration System

## Project Structure
```
mini_project/
│
├── etl/                    # Main package
│   ├── __init__.py        # Package initialization
│   ├── core.py            # Core components (Pipeline, Task)
│   ├── database.py        # Database operations
│   ├── metrics.py         # Metrics collection
│   ├── tasks.py           # Task implementations
│   └── orchestrator.py    # Pipeline orchestration
│
├── tests/                 # Test suite
│   ├── __init__.py
│   └── test_main.py      # Main tests
│
├── main.py               # Entry point
├── requirements.txt      # Dependencies
└── README.md            # Documentation
```

## Features

### Core Components
1. **Task System**
   - Abstract base class for all tasks
   - Automatic retry mechanism
   - Performance metrics collection
   - Error handling

2. **Pipeline Management**
   - Task dependency resolution
   - State management
   - Progress tracking
   - Error recovery

3. **Metrics & Monitoring**
   - Prometheus integration
   - StatsD support
   - Task duration tracking
   - Error rate monitoring
   - Success/failure metrics

4. **Database Integration**
   - Pipeline run tracking
   - Task state persistence
   - Performance metrics storage
   - Error logging

### Workflow Engine Support
1. **Airflow**
   - DAG generation
   - Operator mapping
   - XCom integration
   - Task dependencies

2. **Prefect**
   - Flow definition
   - Task mapping
   - State handlers
   - Retries configuration

3. **Luigi**
   - Task dependencies
   - Target management
   - Parameter handling
   - Workflow scheduling

## Database Schema

### Tables
1. **raw_data**
   - Stores incoming raw data
   - Tracks source and timing
   - JSON content support

2. **transformed_data**
   - Stores processed data
   - Links to raw data
   - Validation status

3. **pipeline_runs**
   - Tracks pipeline executions
   - Performance metrics
   - Error information

4. **pipeline_tasks**
   - Individual task runs
   - Task-level metrics
   - Retry information

## Installation

1. **Environment Setup**
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

2. **Install Dependencies**
```bash
pip install -r requirements.txt
```

3. **Environment Configuration**
```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your settings:
DATABASE_URL=postgresql://user:pass@localhost:5432/etl_db
METRICS_PORT=8000
STATSD_HOST=localhost
STATSD_PORT=8125
```

## Usage

### Basic Usage
```bash
# Run with default engine (Airflow)
python main.py

# Run with specific engine
python main.py --engine prefect
python main.py --engine luigi
```

### Monitoring
1. **Prometheus Metrics**
   - Available at: http://localhost:8000/metrics
   - Key metrics:
     - pipeline_runs_total
     - task_duration_seconds
     - pipeline_errors_total

2. **StatsD Metrics**
   - Task success/failure rates
   - Pipeline performance
   - Custom metrics

### Error Recovery
- Automatic task retries
- State persistence
- Error logging
- Failure notifications

## Development

### Adding New Tasks
1. Create new task class in `etl/tasks.py`:
```python
from etl.core import Task

class NewTask(Task):
    def execute(self, context):
        # Implementation
        return {'result': 'value'}
```

2. Add to pipeline in `etl/orchestrator.py`:
```python
tasks = [
    ExtractTask('extract'),
    NewTask('new_task'),
    LoadTask('load')
]
```

### Running Tests
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_main.py

# Run with coverage
pytest --cov=etl tests/
```

## Performance Optimization
- Batch processing
- Parallel execution
- Caching strategies
- Resource management

## Contributing
1. Fork the repository
2. Create feature branch
3. Commit changes
4. Create pull request

## License
MIT License 