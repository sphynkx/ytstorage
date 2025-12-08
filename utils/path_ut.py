import os
from pathlib import Path
from typing import Optional

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

def safe_join(base: str, *paths: str) -> str:
    """
    Safe joins root and rel paaths, premits Path Traversal
    
    Args:
        base: abs path to storage.
        paths: rel path parts.
        
    Returns:
        abs path.
        
    Raises:
        PermissionError: abs path is out of base.
    """
    base_path = Path(base).resolve()
    final_path = base_path.joinpath(*paths).resolve()
    
    if not str(final_path).startswith(str(base_path)):
        raise PermissionError(f"Path traversal detected: {final_path}")
        
    return str(final_path)