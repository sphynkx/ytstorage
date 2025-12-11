import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# --- S3 Connection Settings ---

# Endpoint URL (Required for Ceph/MinIO)
# For real AWS, leave empty or specific URL.
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://127.0.0.1:9000")

# Credentials
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "admin")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "SECRET")

# Bucket Name (Must exist or be created manually usually, but we can try to create)
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "yurtube-bucket")

# Region. For `rclone` configuration - must need set "us-east-1" !!
S3_REGION_NAME = os.getenv("S3_REGION_NAME", "us-east-1")

# Chunk size for uploads/downloads
CHUNK_SIZE = 5 * 1024 * 1024  # 5MB is good for S3 Multipart