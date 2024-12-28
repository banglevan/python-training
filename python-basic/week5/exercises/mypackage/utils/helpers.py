"""
Helper utilities
"""

from typing import Any, Callable
from functools import wraps
import time
from ..exceptions import RetryError

def retry(
    max_tries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    Decorator thử lại khi lỗi.
    
    Args:
        max_tries: Số lần thử tối đa
        delay: Thời gian chờ ban đầu
        backoff: Hệ số tăng thời gian chờ
        exceptions: Tuple các exception cần thử lại
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            tries = 0
            cur_delay = delay
            
            while tries < max_tries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    tries += 1
                    if tries == max_tries:
                        raise RetryError(
                            f"Failed after {max_tries} tries: {e}"
                        )
                    time.sleep(cur_delay)
                    cur_delay *= backoff
                    
            return None
        return wrapper
    return decorator

def memoize(func: Callable) -> Callable:
    """
    Decorator cache kết quả hàm.
    
    Args:
        func: Hàm cần cache
    """
    cache = {}
    
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        # Create cache key from arguments
        key = str(args) + str(sorted(kwargs.items()))
        
        # Return cached result if exists
        if key in cache:
            return cache[key]
            
        # Calculate and cache result
        result = func(*args, **kwargs)
        cache[key] = result
        return result
        
    # Add clear cache method
    def clear_cache():
        cache.clear()
    wrapper.clear_cache = clear_cache
    
    return wrapper 