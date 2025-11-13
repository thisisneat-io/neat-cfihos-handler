"""Logging utilities for the CFIHOS framework.

This module provides helper functions for initializing and configuring loggers
with standardized formatting and log level handling. It is intended to be used
across the framework to ensure consistent logging behavior.
"""

import logging
import sys


def log_level_converter(log_level):
    """Convert log level string to corresponding logging level integer.

    Args:
        log_level (str): Log level as string. Accepts full names ('debug', 'info',
                        'warning', 'error') or single letters ('d', 'i', 'w', 'e').
                        Case insensitive.

    Returns:
        int: Corresponding logging level integer (10, 20, 30, or 40).

    Raises:
        KeyError: If the provided log_level is not recognized.

    Example:
        >>> log_level_converter('debug')
        10
        >>> log_level_converter('e')
        40
    """
    convert_dict = {
        "debug": 10,
        "info": 20,
        "warning": 30,
        "error": 40,
        "d": 10,
        "i": 20,
        "w": 30,
        "e": 40,
    }

    return convert_dict[log_level]


def log_init(processor_path: str, log_level: str) -> logging.Logger:
    """Initialize and configure a logger for a processor.

    Creates a logger with the specified name and log level, configured with
    a StreamHandler that outputs to stdout using a specific format.

    Args:
        processor_path (str): Name/path for the logger, typically the processor's
                             module path or identifier.
        log_level (str): Desired log level as string. Accepts full names ('debug',
                        'info', 'warning', 'error') or single letters ('d', 'i', 'w', 'e').
                        Case insensitive.

    Returns:
        logging.Logger: Configured logger instance ready for use.

    Example:
        >>> logger = log_init('my_processor', 'info')
        >>> logger.info('This is a test message')
        2023-01-01 12:00:00,000 my_processor INFO     MainProcess 42 This is a test message
    """
    logger = logging.getLogger(processor_path)
    logger.setLevel(level=log_level_converter(log_level.lower()))
    log_handler = logging.StreamHandler(sys.stdout)
    log_formatter = logging.Formatter(
        "%(asctime)s %(name)-12s %(levelname)-8s %(processName)s %(lineno)s %(message)s"
    )
    log_handler.setFormatter(log_formatter)
    logger.addHandler(log_handler)
    return logger
