from config import config
from drivers.driver_base_drv import StorageDriver
from drivers.fs.fs_driver_drv import FSDriver

def get_driver() -> StorageDriver:
    """
    Driver factory. Creates a driver instance based on config.DRIVER_KIND.
    """
    kind = config.DRIVER_KIND.lower()
    
    if kind == "fs":
        return FSDriver()
    
    # Placeholder for future drivers
    # elif kind == "ceph":
    #     return CephDriver(...)
    
    raise ValueError(f"Unknown driver kind: {kind}")