"""
Advanced Prefect features exercise.
"""

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.logging import get_run_logger
from prefect.notifications import SlackWebhook
from prefect.artifacts import create_markdown_artifact
from datetime import timedelta
import pandas as pd
from typing import List, Dict, Any
import json
import hashlib

# Configure notifications
slack_webhook = SlackWebhook(
    url="https://hooks.slack.com/services/XXX/YYY/ZZZ"
)

@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=24),
    retries=3
)
def fetch_batch(batch_id: int) -> pd.DataFrame:
    """Fetch data batch."""
    logger = get_run_logger()
    try:
        # Simulate API call
        data = [
            {"id": i, "value": i * 10}
            for i in range(batch_id * 100, (batch_id + 1) * 100)
        ]
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Batch {batch_id} fetch failed: {e}")
        raise

@task(cache_key_fn=task_input_hash)
def process_batch(df: pd.DataFrame) -> Dict[str, Any]:
    """Process data batch."""
    logger = get_run_logger()
    try:
        results = {
            "count": len(df),
            "sum": df["value"].sum(),
            "mean": df["value"].mean(),
            "min": df["value"].min(),
            "max": df["value"].max()
        }
        
        # Create artifact
        artifact_content = f"""
        ## Batch Processing Results
        
        - Record count: {results['count']}
        - Total value: {results['sum']}
        - Average value: {results['mean']:.2f}
        - Value range: {results['min']} to {results['max']}
        """
        create_markdown_artifact(
            key="batch-results",
            markdown=artifact_content
        )
        
        return results
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        raise

@task
def aggregate_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate batch results."""
    logger = get_run_logger()
    try:
        total_count = sum(r["count"] for r in results)
        total_sum = sum(r["sum"] for r in results)
        total_mean = total_sum / total_count if total_count > 0 else 0
        total_min = min(r["min"] for r in results)
        total_max = max(r["max"] for r in results)
        
        return {
            "total_count": total_count,
            "total_sum": total_sum,
            "overall_mean": total_mean,
            "overall_min": total_min,
            "overall_max": total_max
        }
    except Exception as e:
        logger.error(f"Results aggregation failed: {e}")
        raise

@task(retries=2)
def save_results(results: Dict[str, Any], output_path: str) -> None:
    """Save processing results."""
    logger = get_run_logger()
    try:
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Results saved to {output_path}")
    except Exception as e:
        logger.error(f"Results saving failed: {e}")
        raise

@flow(
    name="batch_processing",
    description="Process data in batches with advanced features",
    version="1.0"
)
def process_batches(
    num_batches: int,
    output_path: str
) -> None:
    """Main batch processing flow."""
    logger = get_run_logger()
    
    try:
        # Parallel batch processing with mapping
        batch_results = process_batch.map(
            fetch_batch.map(range(num_batches))
        )
        
        # Aggregate results
        final_results = aggregate_results(batch_results)
        
        # Save results
        save_results(final_results, output_path)
        
        # Send notification
        slack_webhook.notify(
            f"Batch processing completed successfully!\n"
            f"Processed {final_results['total_count']} records\n"
            f"Total value: {final_results['total_sum']}"
        )
        
        # Create final artifact
        artifact_content = f"""
        # Batch Processing Summary
        
        ## Overview
        - Total batches: {num_batches}
        - Total records: {final_results['total_count']}
        
        ## Results
        - Total value: {final_results['total_sum']}
        - Average value: {final_results['overall_mean']:.2f}
        - Value range: {final_results['overall_min']} to {final_results['overall_max']}
        
        ## Performance
        - Processing time: {flow.start_time - flow.end_time}
        - Average batch size: {final_results['total_count'] / num_batches:.0f}
        """
        create_markdown_artifact(
            key="processing-summary",
            markdown=artifact_content
        )
        
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        slack_webhook.notify(f"❌ Batch processing failed: {e}")
        raise

if __name__ == "__main__":
    # Run flow
    process_batches(
        num_batches=5,
        output_path="output/results.json"
    ) 