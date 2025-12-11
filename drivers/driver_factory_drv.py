from config import config
from drivers.driver_base_drv import StorageDriver
from drivers.fs.fs_driver_drv import FSDriver
# Import inside function to avoid ImportError if deps are missing
# but better here for clarity. Assuming requirements installed.

def get_driver() -> StorageDriver:
    """
    Factory creating driver instance based on config.DRIVER_KIND.
    """
    kind = config.DRIVER_KIND.lower()
    
    if kind == "fs":
        return FSDriver()
    
    elif kind == "s3":
        from drivers.s3.s3_driver_drv import S3Driver
        return S3Driver()
    
    raise ValueError(f"Unknown driver kind: {kind}")