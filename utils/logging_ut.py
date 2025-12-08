import logging
import sys

def setup_logging() -> None:
    """
    Base logging: [TIME] [LEVEL] [LOGGER]: MESSAGE
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    # To mute some libs, ex.:
    # logging.getLogger("grpc").setLevel(logging.WARNING)

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)