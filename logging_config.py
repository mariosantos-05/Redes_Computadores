import logging
import sys

def setup_logger(level_name: str = "INFO") -> logging.Logger:
    """
    Configures a root logger with dual sinks: stdout and a file.
    """
    numeric_level = getattr(logging, level_name.upper(), logging.INFO)
    
    logger = logging.getLogger()
    logger.setLevel(numeric_level)

    # Clear existing handlers if any
    if logger.hasHandlers():
        logger.handlers.clear()

    # Formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console Handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    
    # File Handler (p2p.log)
    file_handler = logging.FileHandler("p2p.log", mode='a')
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
