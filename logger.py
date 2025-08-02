import sys
from loguru import logger

logger.remove()

log_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)

logger.add(sys.stderr, level="DEBUG", format=log_format, colorize=True)
# logger.add(sys.stdout, level="INFO", format=log_format, colorize=True)
