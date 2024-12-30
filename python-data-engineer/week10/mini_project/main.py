"""Main entry point for ETL orchestration system."""

import argparse
from datetime import datetime
import logging
from dotenv import load_dotenv
from etl import Orchestrator

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='ETL Orchestration System')
    parser.add_argument(
        '--engine',
        choices=['airflow', 'prefect', 'luigi'],
        default='airflow',
        help='Workflow engine to use'
    )
    args = parser.parse_args()
    
    try:
        orchestrator = Orchestrator(args.engine)
        context = {
            'source_system': 'example_source',
            'execution_date': datetime.now()
        }
        result = orchestrator.run_pipeline('example_pipeline', context)
        logger.info(f"Pipeline completed successfully: {result}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main() 