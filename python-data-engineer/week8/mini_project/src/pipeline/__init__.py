"""
Data pipeline package.

This package provides components for data processing pipeline:
- Kafka: Message streaming
- Flink: Stream processing
- Cache: State management
"""

from typing import Dict, Any, Optional, Type
import logging
from enum import Enum

from .kafka_manager import KafkaManager
from .flink_processor import FlinkProcessor
from .cache_manager import CacheManager

logger = logging.getLogger(__name__)

class ProcessorType(Enum):
    """Supported processor types."""
    KAFKA = 'kafka'
    FLINK = 'flink'
    CACHE = 'cache'

# Processor registry
PROCESSOR_REGISTRY = {
    ProcessorType.KAFKA: KafkaManager,
    ProcessorType.FLINK: FlinkProcessor,
    ProcessorType.CACHE: CacheManager
}

def create_processor(
    processor_type: str,
    config: Dict[str, Any]
) -> Optional[Any]:
    """
    Create a pipeline processor instance.

    Args:
        processor_type: Type of processor
        config: Processor configuration

    Returns:
        Processor instance or None if creation fails
    """
    try:
        # Validate processor type
        try:
            proc_type = ProcessorType(processor_type.lower())
        except ValueError:
            logger.error(f"Unsupported processor type: {processor_type}")
            return None

        # Get processor class
        processor_class = PROCESSOR_REGISTRY.get(proc_type)
        if not processor_class:
            logger.error(f"No implementation for: {processor_type}")
            return None

        return processor_class(config)

    except Exception as e:
        logger.error(f"Processor creation failed: {e}")
        return None

__all__ = [
    'KafkaManager',
    'FlinkProcessor',
    'CacheManager',
    'ProcessorType',
    'create_processor'
] 