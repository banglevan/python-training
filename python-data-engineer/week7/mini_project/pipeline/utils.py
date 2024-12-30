"""
Utilities Module
------------

Common utilities and helpers.
"""

import logging
from functools import wraps
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    before_log,
    after_log
)

logger = logging.getLogger(__name__)

def retry_with_logging(func):
    """Retry decorator with logging."""
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        before=before_log(logger, logging.INFO),
        after=after_log(logger, logging.ERROR)
    )
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper 