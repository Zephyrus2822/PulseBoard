# Logging configuration
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from colorlog import ColoredFormatter
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

BASE_LOG_DIR = os.getenv("BASE_LOG_DIR", "logs")
_initialized_loggers = set()


def setup_logger(
    logger_name="pulseboard",
    log_file=None,
    log_level=logging.INFO,
    color=True,
    max_file_size=10 * 1024 * 1024,
    backup_count=5,
):
    """
    Set up a logger with console and file handlers
    
    Args:
        logger_name: Name of the logger
        log_file: Path to log file (optional, auto-generated if not provided)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        color: Enable colored console output
        max_file_size: Maximum file size before rotation (bytes)
        backup_count: Number of backup files to keep
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Return existing logger if already initialized
    if logger_name in _initialized_loggers:
        return logging.getLogger(logger_name)

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    logger.propagate = False
    logger.handlers = []

    log_format = "%(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Console handler with colors
    console_handler = logging.StreamHandler()
    if color:
        formatter = ColoredFormatter(
            "%(log_color)s" + log_format,
            datefmt=date_format,
            log_colors={
                "DEBUG": "blue",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )
    else:
        formatter = logging.Formatter(log_format, datefmt=date_format)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler with rotation (daily at midnight)
    if not log_file:
        os.makedirs(BASE_LOG_DIR, exist_ok=True)
        log_file = os.path.join(BASE_LOG_DIR, f"{logger_name}.log")
    else:
        log_dir = os.path.dirname(log_file)
        os.makedirs(log_dir, exist_ok=True)

    file_log_format = "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d - %(funcName)s()] - %(message)s"
    file_handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_formatter = logging.Formatter(file_log_format, datefmt=date_format)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    _initialized_loggers.add(logger_name)
    return logger


def get_logger(
    name=None,
    log_file=None,
    log_level=logging.INFO,
    color=True,
    max_file_size=10 * 1024 * 1024,
    backup_count=5,
):
    """
    Get or create a logger instance
    Auto-detects module name if name not provided
    
    Args:
        name: Logger name (auto-detected from calling module if None)
        log_file: Path to log file
        log_level: Logging level
        color: Enable colored output
        max_file_size: Max file size before rotation
        backup_count: Number of backup files
        
    Returns:
        logging.Logger: Logger instance
    """
    if name is None:
        import inspect

        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        name = module.__name__ if module else "pulseboard"

    return setup_logger(
        logger_name=name,
        log_file=log_file,
        log_level=log_level,
        color=color,
        max_file_size=max_file_size,
        backup_count=backup_count,
    )