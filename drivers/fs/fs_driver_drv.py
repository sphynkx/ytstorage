import os
import shutil
import asyncio
import aiofiles
from aiofiles.os import stat as aio_stat
from typing import AsyncIterator, List

from drivers.driver_base_drv import StorageDriver, FileStat
from config import fs_cfg
from utils.path_ut import safe_join
from utils import logging_ut

logger = logging_ut.get_logger("fs_driver")

class FSDriver(StorageDriver):
    """
    Implementation for local filesystem driver.
    """

    def __init__(self):
        self.root = fs_cfg.FS_ROOT

    def _full_path(self, rel_path: str) -> str:
        """Safely get abs path."""
        return safe_join(self.root, rel_path)

    async def init(self) -> None:
        """Create root dir if not exist."""
        if not os.path.exists(self.root):
            logger.info(f"Creating storage root: {self.root}")
            os.makedirs(self.root, exist_ok=True)
        else:
            logger.info(f"Storage root exists: {self.root}")

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
            etag=None
        )

    async def exists(self, rel_path: str) -> bool:
        full_path = self._full_path(rel_path)
        return await asyncio.to_thread(os.path.exists, full_path)

    async def listdir(self, rel_path: str) -> List[FileStat]:
        full_path = self._full_path(rel_path)
        
        entries = await asyncio.to_thread(os.listdir, full_path)
        
        results = []
        for name in entries:
            # Rel path for child
            # For root rel_path may be empty!!
            child_rel = os.path.join(rel_path, name)
            
            # Stat for each item.
            try:
                stat_res = await self.stat(child_rel)
                results.append(stat_res)
            except FileNotFoundError:
                continue
                
        return results

    async def mkdirs(self, rel_path: str, exist_ok: bool = False) -> None:
        full_path = self._full_path(rel_path)
        await asyncio.to_thread(os.makedirs, full_path, exist_ok=exist_ok)

    async def rename(self, src: str, dst: str, overwrite: bool = False) -> None:
        full_src = self._full_path(src)
        full_dst = self._full_path(dst)
        
        if not overwrite:
            if await asyncio.to_thread(os.path.exists, full_dst):
                 raise FileExistsError(f"Destination already exists: {dst}")
        
        await asyncio.to_thread(os.rename, full_src, full_dst)

    async def remove(self, rel_path: str, recursive: bool = False) -> None:
        full_path = self._full_path(rel_path)
        
        is_dir = await asyncio.to_thread(os.path.isdir, full_path)
        if is_dir:
            if recursive:
                await asyncio.to_thread(shutil.rmtree, full_path)
            else:
                await asyncio.to_thread(os.rmdir, full_path)
        else:
            await asyncio.to_thread(os.remove, full_path)

    async def read_stream(self, rel_path: str, offset: int = 0, length: int = 0) -> AsyncIterator[bytes]:
        full_path = self._full_path(rel_path)
        chunk_size = fs_cfg.CHUNK_SIZE
        
        async with aiofiles.open(full_path, mode='rb') as f:
            if offset > 0:
                await f.seek(offset)
            
            bytes_read = 0
            while True:
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
        
        # Check existance to permit clean of 'wb'
        if not overwrite and not append:
            if await asyncio.to_thread(os.path.exists, full_path):
                 raise FileExistsError(f"File exists: {rel_path}")
        
        # Create parent dirs
        parent_dir = os.path.dirname(full_path)
        if not await asyncio.to_thread(os.path.exists, parent_dir):
            await asyncio.to_thread(os.makedirs, parent_dir, exist_ok=True)

        mode = 'ab' if append else 'wb'
        
        async with aiofiles.open(full_path, mode=mode) as f:
            async for chunk in data_stream:
                await f.write(chunk)