import logging
import grpc
from typing import Optional
from config import config

logger = logging.getLogger("auth_ut")

def validate_token(metadata: grpc.aio.Metadata) -> bool:
    """
    Check and validate Bearer token.
    
    Args:
        metadata: gRPC metadata context.invocation_metadata()
        
    Returns:
        True, if token valid or auth is off.
        False, if token invalid/absent (while auth is on).
    """
    if not config.STORAGE_REMOTE_TOKEN:
        return True

    expected_token = config.STORAGE_REMOTE_TOKEN

    auth_header: Optional[str] = None
    for key, value in metadata:
        if key.lower() == "authorization":
            auth_header = value
            break

    if not auth_header:
        logger.warning("Missing authorization header")
        return False

    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        logger.warning("Invalid authorization header format")
        return False

    if parts[1] != expected_token:
        logger.warning("Invalid token provided")
        return False

    return True