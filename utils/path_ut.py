import os
from pathlib import Path


def normalize_path(rel_path: str) -> str:
    """
    Normalize rel path API:
    - remove leading "/"
    - replace "\" -> "/"
    - Trim
    
    Args:
        rel_path: raw path from request.
        
    Returns:
        valid rel path.
    """
    if not rel_path:
        return ""
    
    clean = rel_path.replace("\\", "/").strip()
    return clean.lstrip("/")


def safe_join(base: str, rel_path: str) -> str:
    """
    Safe joins root and rel paths, premits Path Traversal
    
    Args:
        base: abs path to storage.
        rel_path: rel path from client.
        
    Returns:
        abs path.
        
    Raises:
        PermissionError: abs path is out of base.
    """
    base_path = Path(base).resolve()
    clean_rel = normalize_path(rel_path)
    
    # Use pathlib to join and resolve
    final_path = (base_path / clean_rel).resolve()
    
    # Check if the final path is still inside base_path
    if not str(final_path).startswith(str(base_path)):
        raise PermissionError(f"Path traversal detected: {rel_path}")
        
    return str(final_path)