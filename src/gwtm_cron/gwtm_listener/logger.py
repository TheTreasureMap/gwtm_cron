"""
Structured JSON logging for Kubernetes deployment.

This module provides JSON-formatted logging that integrates well with
Kubernetes log aggregation systems (ELK, Loki, etc.).

Usage:
    from gwtm_listener import logger

    log = logger.get_logger(__name__)
    log.info("Processing alert", extra={"alert_id": "S230518h", "type": "LIGO"})
"""

import logging
import json
import sys
import os
from datetime import datetime


class JsonFormatter(logging.Formatter):
    """Format log records as JSON for structured logging"""

    def format(self, record):
        log_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'message': record.getMessage(),
            'logger': record.name,
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }

        # Add extra fields if provided
        if hasattr(record, 'alert_id'):
            log_data['alert_id'] = record.alert_id
        if hasattr(record, 'listener_type'):
            log_data['listener_type'] = record.listener_type
        if hasattr(record, 'event_type'):
            log_data['event_type'] = record.event_type

        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def get_logger(name: str, level: str = None) -> logging.Logger:
    """
    Get or create a structured logger.

    Args:
        name: Logger name (typically __name__)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
               If not provided, reads from LOG_LEVEL environment variable,
               defaults to INFO

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Only configure if not already configured
    if not logger.handlers:
        # Determine log level
        if level is None:
            level = os.environ.get('LOG_LEVEL', 'INFO').upper()

        logger.setLevel(getattr(logging, level))

        # Create handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

        # Prevent propagation to avoid duplicate logs
        logger.propagate = False

    return logger


# Convenience function for backward compatibility with print statements
def log_info(message: str, **kwargs):
    """Quick info log (for migration from print statements)"""
    logger = get_logger('gwtm_cron')
    logger.info(message, extra=kwargs)


def log_error(message: str, **kwargs):
    """Quick error log"""
    logger = get_logger('gwtm_cron')
    logger.error(message, extra=kwargs)


def log_warning(message: str, **kwargs):
    """Quick warning log"""
    logger = get_logger('gwtm_cron')
    logger.warning(message, extra=kwargs)
