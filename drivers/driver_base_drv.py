from abc import ABC, abstractmethod
from typing import AsyncIterator, List, Optional
from dataclasses import dataclass

@dataclass
class FileStat:
    name: str
    rel_path: str
    is_dir: bool
    size: int
    created_at: float
    updated_at: float
    etag: Optional[str] = None

class StorageDriver(ABC):
    """
    Abstract interface for storage drivers (fs, s3, etc).
    """

    @abstractmethod
    async def init(self) -> None:
        pass

    @abstractmethod
    async def stat(self, rel_path: str) -> FileStat:
        """Get metadata about file/dir."""
        pass

    @abstractmethod
    async def exists(self, rel_path: str) -> bool:
        pass

    @abstractmethod
    async def listdir(self, rel_path: str) -> List[FileStat]:
        pass

    @abstractmethod
    async def mkdirs(self, rel_path: str, exist_ok: bool = False) -> None:
        pass

    @abstractmethod
    async def rename(self, src: str, dst: str, overwrite: bool = False) -> None:
        pass

    @abstractmethod
    async def remove(self, rel_path: str, recursive: bool = False) -> None:
        """Remove file/dir."""
        pass

    @abstractmethod
    async def read_stream(self, rel_path: str, offset: int = 0, length: int = 0) -> AsyncIterator[bytes]:
        """
        Read file stream.
        length=0 - read to end.
        """
        pass # yield bytes

    @abstractmethod
    async def write_stream(self, rel_path: str, data_stream: AsyncIterator[bytes], overwrite: bool = False, append: bool = False) -> None:
        """
        Write stream to file.
        """
        pass