"""
Performance tests for system throughput.
"""

import pytest
import time
from concurrent.futures import ThreadPoolExecutor
from src.utils.config import Config
from src.ingestion.producers.event_producer import EventProducer
from src.utils.metrics import MetricsTracker

@pytest.fixture
def config():
    """Test configuration."""
    return Config("tests/fixtures/config")

def test_producer_throughput(config):
    """Test producer throughput."""
    producer = EventProducer(config)
    metrics = MetricsTracker()
    
    num_events = 10000
    start_time = time.time()
    
    # Generate and send events
    for _ in range(num_events):
        event = producer.generate_event()
        producer.send_event(event)
    
    duration = time.time() - start_time
    events_per_second = num_events / duration
    
    assert events_per_second >= 1000  # At least 1000 events/second

def test_parallel_throughput(config):
    """Test parallel throughput."""
    num_threads = 4
    num_events_per_thread = 2500
    metrics = MetricsTracker()
    
    def produce_events():
        producer = EventProducer(config)
        for _ in range(num_events_per_thread):
            event = producer.generate_event()
            producer.send_event(event)
    
    start_time = time.time()
    
    # Run producers in parallel
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(produce_events)
            for _ in range(num_threads)
        ]
        
        # Wait for completion
        for future in futures:
            future.result()
    
    duration = time.time() - start_time
    total_events = num_threads * num_events_per_thread
    events_per_second = total_events / duration
    
    assert events_per_second >= 2000  # At least 2000 events/second 