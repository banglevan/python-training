"""
Performance tests for feature serving.
"""

import pytest
import time
import concurrent.futures
from src.serving.client.feature_client import FeatureClient

def test_online_serving_latency():
    """Test online feature serving latency."""
    client = FeatureClient("http://localhost:8000")
    latencies = []
    
    def get_features():
        start = time.time()
        response = client.get_online_features(
            entity_type="customer",
            entity_ids=["1", "2", "3"],
            features=["total_orders", "avg_order_value"]
        )
        return time.time() - start
    
    # Test with 100 concurrent requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_latency = {
            executor.submit(get_features): i 
            for i in range(100)
        }
        
        for future in concurrent.futures.as_completed(future_to_latency):
            latencies.append(future.result())
    
    avg_latency = sum(latencies) / len(latencies)
    p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
    
    assert avg_latency < 0.1  # Average latency under 100ms
    assert p95_latency < 0.2  # 95th percentile under 200ms 