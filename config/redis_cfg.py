import os
from dotenv import load_dotenv

load_dotenv()

USE_REDIS_CACHE = os.getenv("USE_REDIS_CACHE", "true").lower() == "true"

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

CACHE_TTL_META = int(os.getenv("CACHE_TTL_META", "600"))

CACHE_TTL_DATA = int(os.getenv("CACHE_TTL_DATA", "3600"))

# Max size for caching (1Mb). Bigger files - from S3 directly.
CACHE_MAX_FILE_SIZE = int(os.getenv("CACHE_MAX_FILE_SIZE", "1048576")) 