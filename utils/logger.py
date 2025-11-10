import logging
from logging.handlers import RotatingFileHandler


def setup_logging(config):
    """Thiết lập hệ thống logging"""
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, config.log_level))

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = RotatingFileHandler(
        config.log_file,
        maxBytes=config.max_file_size,
        backupCount=config.backup_count,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Logger cho signals
    signals_logger = logging.getLogger('signals')
    signals_logger.setLevel(logging.INFO)
    signals_handler = RotatingFileHandler(
        config.signals_log_file,
        maxBytes=config.max_file_size,
        backupCount=config.backup_count,
        encoding='utf-8'
    )
    signals_handler.setFormatter(formatter)
    signals_logger.addHandler(signals_handler)
    signals_logger.propagate = False