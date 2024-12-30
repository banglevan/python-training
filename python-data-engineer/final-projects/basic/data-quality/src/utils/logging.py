"""
Logging configuration module.
"""

import logging
import logging.handlers
from pathlib import Path
from datetime import datetime
import json
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
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': self.formatException(record.exc_info)
            }
        
        # Add extra fields
        if hasattr(record, 'extra'):
            log_data.update(record.extra)
        
        return json.dumps(log_data)

def setup_logging(
    log_dir: str = "logs",
    level: str = "INFO",
    retention: int = 30,
    json_format: bool = True
) -> None:
    """
    Set up logging configuration.
    
    Args:
        log_dir: Log directory
        level: Logging level
        retention: Log retention days
        json_format: Whether to use JSON format
    """
    try:
        # Create log directory
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        # Set up root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        
        # Create formatters
        if json_format:
            formatter = JsonFormatter()
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        
        # Create handlers
        handlers = [
            # File handler with rotation
            logging.handlers.TimedRotatingFileHandler(
                log_path / "quality.log",
                when="midnight",
                interval=1,
                backupCount=retention
            ),
            # Error file handler
            logging.handlers.RotatingFileHandler(
                log_path / "error.log",
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5
            ),
            # Console handler
            logging.StreamHandler()
        ]
        
        # Configure handlers
        for handler in handlers:
            handler.setFormatter(formatter)
            if isinstance(handler, logging.handlers.RotatingFileHandler):
                handler.setLevel(logging.ERROR)
            root_logger.addHandler(handler)
        
        logging.info("Logging system initialized")
        
    except Exception as e:
        print(f"Failed to initialize logging: {e}")
        raise

class LoggerAdapter(logging.LoggerAdapter):
    """Custom logger adapter with context."""
    
    def process(
        self,
        msg: str,
        kwargs: Dict[str, Any]
    ) -> tuple:
        """Process log message with context."""
        # Add context to extra
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        kwargs['extra'].update(self.extra)
        
        return msg, kwargs

def get_logger(
    name: str,
    context: Dict[str, Any] = None
) -> logging.Logger:
    """
    Get logger instance with context.
    
    Args:
        name: Logger name
        context: Additional context
    
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    if context:
        return LoggerAdapter(logger, context)
    return logger 