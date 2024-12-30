"""
Logging configuration module.
"""

import logging
import logging.handlers
from pathlib import Path
import json
from datetime import datetime
from typing import Dict, Any

class JsonFormatter(logging.Formatter):
    """JSON log formatter."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': self.formatException(record.exc_info)
            }
        
        if hasattr(record, 'extra'):
            log_data.update(record.extra)
        
        return json.dumps(log_data)

def setup_logging(
    log_dir: str = "logs",
    level: str = "INFO",
    json_format: bool = True
) -> None:
    """
    Set up logging configuration.
    
    Args:
        log_dir: Log directory
        level: Log level
        json_format: Whether to use JSON format
    """
    try:
        # Create log directory
        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True)
        
        # Set up handlers
        handlers = []
        
        # File handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_path / "analytics.log",
            maxBytes=10485760,  # 10MB
            backupCount=5
        )
        handlers.append(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        handlers.append(console_handler)
        
        # Set formatter
        if json_format:
            formatter = JsonFormatter()
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        
        for handler in handlers:
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)
        
        logging.info("Logging system initialized")
        
    except Exception as e:
        print(f"Failed to initialize logging: {e}")
        raise 