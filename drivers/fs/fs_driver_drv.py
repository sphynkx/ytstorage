import os
import shutil
import aiofiles
from aiofiles.os import stat as aio_stat
from typing import AsyncIterator, List
import logging
import asyncio

from config import config
from utils.path_ut import safe_join, normalize_path
from drivers.driver_base_drv import StorageDriver, FileStat

logger = logging.getLogger("fs_driver_drv")

class FSDriver(StorageDriver):
    """
    Storage Driver implementation for the local file system. All operations are restricted to APP_STORAGE_FS_ROOT.
    """

    def __init__(self):
        self.root = config.APP_STORAGE_FS_ROOT

    def _full_path(self, rel_path: str) -> str:
        return safe_join(self.root, normalize_path(rel_path))

    async def init(self) -> None:
        if not os.path.exists(self.root):
            logger.info(f"Root directory {self.root} does not exist, creating...")
            os.makedirs(self.root, exist_ok=True)
        logger.info(f"FSDriver initialized at {self.root}")

    async def stat(self, rel_path: str) -> FileStat:
        full_path = self._full_path(rel_path)
        st = await aio_stat(full_path)
        return FileStat(
            name=os.path.basename(full_path),
            rel_path=rel_path,
            is_dir=os.path.isdir(full_path),
            size=st.st_size,
            created_at=st.st_ctime,
            updated_at=st.st_mtime,
            etag=None # TODO: add hashing
        )

    async def exists(self, rel_path: str) -> bool:
        full_path = self._full_path(rel_path)
        return os.path.exists(full_path)

    async def listdir(self, rel_path: str) -> List[FileStat]:
        full_path = self._full_path(rel_path)
        
        loop = asyncio.get_running_loop()
        entries = await loop.run_in_executor(None, os.listdir, full_path)
        
        results = []
        for name in entries:
            child_rel = os.path.join(rel_path, name)
            try:
                stat_res = await self.stat(child_rel)
                results.append(stat_res)
            except FileNotFoundError:
                continue
        return results

    async def mkdirs(self, rel_path: str, exist_ok: bool = False) -> None:
        full_path = self._full_path(rel_path)
        os.makedirs(full_path, exist_ok=exist_ok)

    async def rename(self, src: str, dst: str, overwrite: bool = False) -> None:
        full_src = self._full_path(src)
        full_dst = self._full_path(dst)
        
        if not overwrite and os.path.exists(full_dst):
             raise FileExistsError(f"Destination exists: {dst}")
             
        os.rename(full_src, full_dst)

    async def remove(self, rel_path: str, recursive: bool = False) -> None:
        full_path = self._full_path(rel_path)
        if os.path.isdir(full_path):
            if recursive:
                shutil.rmtree(full_path)
            else:
                os.rmdir(full_path)
        else:
            os.remove(full_path)

    async def read_stream(self, rel_path: str, offset: int = 0, length: int = 0) -> AsyncIterator[bytes]:
        full_path = self._full_path(rel_path)
        chunk_size = 1024 * 1024 # 1MB
        
        async with aiofiles.open(full_path, mode='rb') as f:
            if offset > 0:
                await f.seek(offset)
            
            bytes_read = 0
            while True:
                # If length is specified, read no more than the remainder
                to_read = chunk_size
                if length > 0:
                    remaining = length - bytes_read
                    if remaining <= 0:
                        break
                    to_read = min(chunk_size, remaining)

                chunk = await f.read(to_read)
                if not chunk:
                    break
                
                yield chunk
                bytes_read += len(chunk)

    async def write_stream(self, rel_path: str, data_stream: AsyncIterator[bytes], overwrite: bool = False, append: bool = False) -> None:
        full_path = self._full_path(rel_path)
        
        if os.path.exists(full_path) and not overwrite and not append:
             raise FileExistsError(f"File exists: {rel_path}")
        
        mode = 'ab' if append else 'wb'
        
        async with aiofiles.open(full_path, mode=mode) as f:
            async for chunk in data_stream:
                await f.write(chunk)