import logging


def get_logger(log_path: str) -> logging.Logger:
    """获取一个logger。"""
    logger = logging.getLogger(__name__)
    handler = logging.FileHandler(log_path)
    formatter = logging.Formatter(
        fmt="[%(asctime)s][%(name)s][%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger