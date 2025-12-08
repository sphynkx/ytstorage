import logging
import grpc
from typing import Optional, NoReturn

logger = logging.getLogger("errors_ut")

def translate_exception(e: Exception) -> tuple[grpc.StatusCode, str]:
    """
    Map python exceptions to gRPC codes/msgs.
    
    Args:
        e: exception
        
    Returns:
        tuple: (grpc.StatusCode, str_message)
    """
    msg = str(e)
    
    if isinstance(e, FileNotFoundError):
        return grpc.StatusCode.NOT_FOUND, f"Resource not found: {msg}"
    
    if isinstance(e, PermissionError):
        return grpc.StatusCode.PERMISSION_DENIED, f"Permission denied: {msg}"
    
    if isinstance(e, FileExistsError):
        return grpc.StatusCode.ALREADY_EXISTS, f"Resource already exists: {msg}"
        
    if isinstance(e, IsADirectoryError):
        return grpc.StatusCode.FAILED_PRECONDITION, f"Is a directory: {msg}"
    
    if isinstance(e, NotADirectoryError):
        return grpc.StatusCode.FAILED_PRECONDITION, f"Not a directory: {msg}"
    
    logger.error(f"Internal error: {e}", exc_info=True)
    return grpc.StatusCode.INTERNAL, f"Internal server error: {msg}"

async def abort(context: grpc.aio.ServicerContext, code: grpc.StatusCode, details: str) -> NoReturn:
    await context.abort(code, details)