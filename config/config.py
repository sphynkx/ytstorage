import os
from dotenv import load_dotenv

# Load .env automatically (looks in current dir and parents)
load_dotenv()

def get_env_bool(key: str, default: bool = False) -> bool:
    val = os.getenv(key, str(default)).lower()
    return val in ('true', '1', 'yes', 'on')

# Listen on
STORAGE_REMOTE_ADDRESS = os.getenv("STORAGE_REMOTE_ADDRESS", "0.0.0.0:50070")

# Auth enable/disable
STORAGE_REMOTE_TLS = get_env_bool("STORAGE_REMOTE_TLS", False)

# Set same as on yurtube side!! (check)
STORAGE_REMOTE_TOKEN = os.getenv("STORAGE_REMOTE_TOKEN", "")

STORAGE_GRPC_MAX_MSG_MB = int(os.getenv("STORAGE_GRPC_MAX_MSG_MB", "64"))

# Driver type: "fs" (MVP), "s3", "ceph", etc (TODOs)
DRIVER_KIND = os.getenv("DRIVER_KIND", "fs")

# Root dir for FS driver.
# Default is /tmp/ytstorage_data but wait for abs path.
DEFAULT_FS_ROOT = os.path.abspath("/tmp/ytstorage_data")
# Note: Actual driver uses config/fs_cfg.py, but we keep this variable 
# here for main server logging or future checks.
APP_STORAGE_FS_ROOT = os.getenv("APP_STORAGE_FS_ROOT", DEFAULT_FS_ROOT)
APP_STORAGE_FS_ROOT = os.path.abspath(APP_STORAGE_FS_ROOT)

VERSION = os.getenv("VERSION", "1.0.0")