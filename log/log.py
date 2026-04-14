from loguru import logger

# File logging setup
logger.add(
    "logs/pipeline.log",
    rotation="1 MB",        # rotate after 1MB
    retention="7 days",     # keep logs 7 days
    level="INFO"
)