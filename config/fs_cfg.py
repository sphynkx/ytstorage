import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env automatically
load_dotenv()

# Get the raw value from environment
_fs_root_env = os.getenv("APP_STORAGE_FS_ROOT")

# Using /tmp/ytstorage_data as a safe default for local dev
_default_root = Path("/tmp/ytstorage_data")

if _fs_root_env:
    FS_ROOT = str(Path(_fs_root_env).resolve())
else:
    FS_ROOT = str(_default_root.resolve())

# Chunks for read/write operations (1MB)
CHUNK_SIZE = 1024 * 1024