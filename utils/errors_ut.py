import logging
import grpc
from typing import NoReturn, Tuple

logger = logging.getLogger("errors_ut")

def translate_exception(e: Exception) -> Tuple[grpc.StatusCode, str]:
    """
    Map Python exceptions to gRPC status codes and messages.
    Does NOT log anticipated errors (Not Found, Exists, etc).
    Logs INTERNAL errors.
    
    Args:
        e: The caught exception.
        
    Returns:
        tuple: (grpc.StatusCode, str_message)
    """
    msg = str(e)
    
    # 1. Not Found
    if isinstance(e, FileNotFoundError):
        return grpc.StatusCode.NOT_FOUND, f"Resource not found: {msg}"
    
    # 2. Permission / Access
    if isinstance(e, PermissionError):
        return grpc.StatusCode.PERMISSION_DENIED, f"Permission denied: {msg}"
    
    # 3. Already Exists
    if isinstance(e, FileExistsError):
        return grpc.StatusCode.ALREADY_EXISTS, f"Resource already exists: {msg}"
        
    # 4. Directory errors (e.g. trying to remove non-empty dir without recursive)
    # [Errno 39] Directory not empty
    if isinstance(e, OSError) and e.errno == 39: 
        return grpc.StatusCode.FAILED_PRECONDITION, f"Directory not empty: {msg}"

    if isinstance(e, IsADirectoryError):
        return grpc.StatusCode.FAILED_PRECONDITION, f"Is a directory: {msg}"
    
    if isinstance(e, NotADirectoryError):
        return grpc.StatusCode.FAILED_PRECONDITION, f"Not a directory: {msg}"
    
    # 5. Internal / Unexpected
    # Only log tracebacks for actual bugs/system failures
    logger.error(f"Internal error: {e}", exc_info=True)
    return grpc.StatusCode.INTERNAL, f"Internal server error: {msg}"

async def abort(context: grpc.aio.ServicerContext, code: grpc.StatusCode, details: str) -> NoReturn:
    """
    Abort RPC with specific status code.
    """
    await context.abort(code, details)