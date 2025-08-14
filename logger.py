import sys
from loguru import logger
import config

logger.remove()

logger.add(sys.stderr, level="DEBUG", format=config.LOG_FORMAT, colorize=True)