"""
Backend Logging Configuration

Provides structured, production-ready logging for the FastAPI backend.
Integrates with Databricks Apps logging and provides clean, readable output.

Features:
- Colored console output for development
- JSON structured logs for production
- Request/response logging
- Performance monitoring
- Error tracking with context
"""

import logging
import sys
import json
from datetime import datetime
from typing import Any, Dict, Optional
import os
from functools import wraps
import time


class ColoredFormatter(logging.Formatter):
    """Colored log formatter for development"""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[34m',       # Blue
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m',       # Reset
        'BOLD': '\033[1m',        # Bold
    }
    
    def format(self, record: logging.LogRecord) -> str:
        # Add color to level name
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = (
                f"{self.COLORS['BOLD']}{self.COLORS[levelname]}"
                f"{levelname:8}"
                f"{self.COLORS['RESET']}"
            )
        
        # Format the message
        formatted = super().format(record)
        
        # Add extra context if available
        if hasattr(record, 'context') and record.context:
            context_str = json.dumps(record.context, indent=2)
            formatted += f"\n  Context: {context_str}"
        
        return formatted


class JSONFormatter(logging.Formatter):
    """JSON formatter for production logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add extra context
        if hasattr(record, 'context'):
            log_data['context'] = record.context
        
        if hasattr(record, 'user_email'):
            log_data['user'] = record.user_email
        
        if hasattr(record, 'request_id'):
            log_data['request_id'] = record.request_id
        
        if hasattr(record, 'duration_ms'):
            log_data['duration_ms'] = record.duration_ms
        
        return json.dumps(log_data)


class ContextLogger(logging.LoggerAdapter):
    """Logger adapter that adds context to all log messages"""
    
    def __init__(self, logger: logging.Logger, component: str):
        super().__init__(logger, {'component': component})
        self.component = component
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        # Add component to extra
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        kwargs['extra']['component'] = self.component
        return msg, kwargs
    
    def with_context(self, **context: Any) -> 'ContextLogger':
        """Add context to log messages"""
        new_extra = {**self.extra, 'context': context}
        return ContextLogger(self.logger, self.component)
    
    def api_call(self, method: str, endpoint: str, status: int, duration_ms: float, 
                 user_email: Optional[str] = None) -> None:
        """Log API calls with metrics"""
        extra = {
            'method': method,
            'endpoint': endpoint,
            'status': status,
            'duration_ms': round(duration_ms, 2),
        }
        if user_email:
            extra['user_email'] = user_email
        
        msg = f"{method} {endpoint} - {status} ({duration_ms:.0f}ms)"
        
        if status >= 500:
            self.error(msg, extra=extra)
        elif status >= 400:
            self.warning(msg, extra=extra)
        elif duration_ms > 3000:
            self.warning(f"{msg} - SLOW REQUEST", extra=extra)
        else:
            self.info(msg, extra=extra)
    
    def query_executed(self, query_type: str, duration_ms: float, 
                      rows: Optional[int] = None, **context: Any) -> None:
        """Log database query execution"""
        extra = {
            'query_type': query_type,
            'duration_ms': round(duration_ms, 2),
            'context': context
        }
        if rows is not None:
            extra['rows'] = rows
        
        msg = f"Query executed: {query_type} ({duration_ms:.0f}ms)"
        if rows is not None:
            msg += f" - {rows} rows"
        
        if duration_ms > 5000:
            self.warning(f"{msg} - SLOW QUERY", extra=extra)
        else:
            self.info(msg, extra=extra)
    
    def user_action(self, action: str, user_email: str, **context: Any) -> None:
        """Log user actions"""
        extra = {
            'action': action,
            'user_email': user_email,
            'context': context
        }
        self.info(f"User action: {action}", extra=extra)


def setup_logging(
    level: str = None,
    format_type: str = None,
    log_file: Optional[str] = None
) -> None:
    """
    Setup application logging configuration
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR). Defaults to env var or INFO
        format_type: 'json' or 'colored'. Defaults to 'colored' for dev, 'json' for prod
        log_file: Optional file path for file logging
    """
    # Determine log level
    if level is None:
        level = os.getenv('LOG_LEVEL', 'INFO')
    log_level = getattr(logging, level.upper())
    
    # Determine format type
    if format_type is None:
        env = os.getenv('ENVIRONMENT', 'development')
        format_type = 'colored' if env == 'development' else 'json'
    
    # Create formatter
    if format_type == 'json':
        formatter = JSONFormatter()
    else:
        formatter = ColoredFormatter(
            fmt='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    # Setup console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()  # Remove existing handlers
    root_logger.addHandler(console_handler)
    
    # Setup file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(JSONFormatter())  # Always use JSON for files
        root_logger.addHandler(file_handler)
    
    # Reduce noise from external libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('databricks').setLevel(logging.INFO)
    logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
    
    # Log that logging is configured
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured: level={level}, format={format_type}")


def get_logger(name: str) -> ContextLogger:
    """
    Get a logger for a specific component
    
    Args:
        name: Component name (e.g., 'api', 'auth', 'genie')
    
    Returns:
        ContextLogger instance
    """
    base_logger = logging.getLogger(name)
    return ContextLogger(base_logger, name)


def log_performance(logger: ContextLogger):
    """
    Decorator to log function execution time
    
    Usage:
        @log_performance(logger)
        async def my_function():
            ...
    """
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                logger.info(
                    f"Function {func.__name__} completed",
                    extra={'duration_ms': round(duration_ms, 2)}
                )
                return result
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                logger.error(
                    f"Function {func.__name__} failed",
                    exc_info=True,
                    extra={'duration_ms': round(duration_ms, 2)}
                )
                raise
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                logger.info(
                    f"Function {func.__name__} completed",
                    extra={'duration_ms': round(duration_ms, 2)}
                )
                return result
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                logger.error(
                    f"Function {func.__name__} failed",
                    exc_info=True,
                    extra={'duration_ms': round(duration_ms, 2)}
                )
                raise
        
        # Return appropriate wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


# Convenience function for measuring operations
class LogTimer:
    """Context manager for timing operations"""
    
    def __init__(self, logger: ContextLogger, operation: str, **context):
        self.logger = logger
        self.operation = operation
        self.context = context
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.debug(f"Starting: {self.operation}", extra={'context': self.context})
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self.start_time) * 1000
        extra = {'duration_ms': round(duration_ms, 2), 'context': self.context}
        
        if exc_type is not None:
            self.logger.error(
                f"Failed: {self.operation}",
                exc_info=(exc_type, exc_val, exc_tb),
                extra=extra
            )
        elif duration_ms > 1000:
            self.logger.warning(f"Slow operation: {self.operation}", extra=extra)
        else:
            self.logger.debug(f"Completed: {self.operation}", extra=extra)
