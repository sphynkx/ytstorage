import asyncio
import os
import time
from typing import AsyncIterator, List, Optional
from contextlib import asynccontextmanager

from aiobotocore.session import get_session
from botocore.exceptions import ClientError
from botocore.config import Config

from drivers.driver_base_drv import StorageDriver, FileStat
from config import s3_cfg
from utils import logging_ut

logger = logging_ut.get_logger("s3_driver")


def _cfg_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def _cfg_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


class S3Driver(StorageDriver):
    """
    S3-compatible driver (Ceph, MinIO, AWS).
    Uses aiobotocore (aiohttp underneath).

    Key fix for MinIO under load:
    - read_stream supports resume (Range) when aiohttp raises ContentLengthError / disconnects.
    """

    def __init__(self):
        self.endpoint = s3_cfg.S3_ENDPOINT_URL
        self.access_key = s3_cfg.S3_ACCESS_KEY_ID
        self.secret_key = s3_cfg.S3_SECRET_ACCESS_KEY
        self.bucket = s3_cfg.S3_BUCKET_NAME
        self.region = s3_cfg.S3_REGION_NAME
        self.session = get_session()

        # chunk size used to read from MinIO (iter_chunks)
        self._read_chunk = _cfg_int("S3_READ_CHUNK_BYTES", getattr(s3_cfg, "CHUNK_SIZE", 1024 * 1024))
        if self._read_chunk <= 0:
            self._read_chunk = 1024 * 1024

        # resume policy
        self._resume_max_attempts = max(1, _cfg_int("S3_READ_RESUME_MAX_ATTEMPTS", 6))
        self._resume_backoff_base = _cfg_float("S3_READ_RESUME_BACKOFF_SEC", 1.0)

        # botocore tuning
        self._botocore_cfg = Config(
            retries={"max_attempts": _cfg_int("S3_MAX_ATTEMPTS", 6), "mode": "standard"},
            connect_timeout=_cfg_float("S3_CONNECT_TIMEOUT_SEC", 10.0),
            read_timeout=_cfg_float("S3_READ_TIMEOUT_SEC", 120.0),
            max_pool_connections=_cfg_int("S3_MAX_POOL_CONNECTIONS", 50),
        )

    @asynccontextmanager
    async def _client(self):
        async with self.session.create_client(
            "s3",
            region_name=self.region,
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=self._botocore_cfg,
        ) as client:
            yield client

    def _clean_key(self, rel_path: str) -> str:
        return rel_path.strip("/")

    async def init(self) -> None:
        logger.info(f"Initializing S3 Driver: {self.endpoint} / {self.bucket}")
        async with self._client() as client:
            await client.head_bucket(Bucket=self.bucket)
        logger.info("S3 Bucket exists.")

    async def stat(self, rel_path: str) -> FileStat:
        key = self._clean_key(rel_path)
        try:
            async with self._client() as client:
                response = await client.head_object(Bucket=self.bucket, Key=key)
                lm = response.get("LastModified")
                ts = lm.timestamp() if lm else 0
                return FileStat(
                    name=os.path.basename(key),
                    rel_path=rel_path,
                    is_dir=False,
                    size=int(response["ContentLength"]),
                    created_at=ts,
                    updated_at=ts,
                    etag=response.get("ETag", "").strip('"'),
                )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                # directory emulation
                if await self._is_dir(key):
                    return FileStat(
                        name=os.path.basename(key.rstrip("/")),
                        rel_path=rel_path,
                        is_dir=True,
                        size=0,
                        created_at=0,
                        updated_at=0,
                    )
                raise FileNotFoundError(f"S3 Object not found: {key}")
            raise

    async def _is_dir(self, prefix: str) -> bool:
        if not prefix.endswith("/"):
            prefix += "/"
        async with self._client() as client:
            res = await client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
            return "Contents" in res or "CommonPrefixes" in res

    async def exists(self, rel_path: str) -> bool:
        try:
            await self.stat(rel_path)
            return True
        except FileNotFoundError:
            return False

    async def listdir(self, rel_path: str) -> List[FileStat]:
        prefix = self._clean_key(rel_path)
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        results: List[FileStat] = []
        async with self._client() as client:
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/"):
                for p in page.get("CommonPrefixes", []):
                    dir_name = p["Prefix"][len(prefix):].strip("/")
                    results.append(FileStat(name=dir_name, rel_path=p["Prefix"].strip("/"), is_dir=True, size=0, created_at=0, updated_at=0))
                for c in page.get("Contents", []):
                    key = c["Key"]
                    if key == prefix:
                        continue
                    name = key[len(prefix):]
                    lm = c.get("LastModified")
                    ts = lm.timestamp() if lm else 0
                    results.append(
                        FileStat(
                            name=name,
                            rel_path=key,
                            is_dir=False,
                            size=int(c.get("Size") or 0),
                            created_at=ts,
                            updated_at=ts,
                            etag=str(c.get("ETag") or "").strip('"') or None,
                        )
                    )
        return results

    async def mkdirs(self, rel_path: str, exist_ok: bool = False) -> None:
        key = self._clean_key(rel_path)
        if not key.endswith("/"):
            key += "/"
        async with self._client() as client:
            await client.put_object(Bucket=self.bucket, Key=key)

    async def rename(self, src: str, dst: str, overwrite: bool = False) -> None:
        src_key = self._clean_key(src)
        dst_key = self._clean_key(dst)
        async with self._client() as client:
            if not overwrite:
                try:
                    await client.head_object(Bucket=self.bucket, Key=dst_key)
                    raise FileExistsError(f"Destination exists: {dst}")
                except ClientError as e:
                    if e.response.get("Error", {}).get("Code") != "404":
                        raise

            copy_source = {"Bucket": self.bucket, "Key": src_key}
            await client.copy_object(Bucket=self.bucket, Key=dst_key, CopySource=copy_source)
            await client.delete_object(Bucket=self.bucket, Key=src_key)

    async def remove(self, rel_path: str, recursive: bool = False) -> None:
        key = self._clean_key(rel_path)
        async with self._client() as client:
            is_file = True
            try:
                await client.head_object(Bucket=self.bucket, Key=key)
            except ClientError:
                is_file = False

            if is_file:
                await client.delete_object(Bucket=self.bucket, Key=key)
                return

            if recursive:
                if not key.endswith("/"):
                    key += "/"
                paginator = client.get_paginator("list_objects_v2")
                async for page in paginator.paginate(Bucket=self.bucket, Prefix=key):
                    if "Contents" in page:
                        objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                        if objects:
                            await client.delete_objects(Bucket=self.bucket, Delete={"Objects": objects})
            else:
                if await self._is_dir(key):
                    raise OSError(39, "Directory not empty (S3 prefix)")

    def _range_header(self, start: int, length: int) -> str:
        if length and length > 0:
            end = start + length - 1
            return f"bytes={start}-{end}"
        return f"bytes={start}-"

    async def read_stream(self, rel_path: str, offset: int = 0, length: int = 0) -> AsyncIterator[bytes]:
        """
        Resumable read stream.

        If underlying aiohttp stream is cut (ContentLengthError, connection lost),
        we retry using Range starting from (offset + bytes_sent).
        """
        key = self._clean_key(rel_path)
        start0 = int(offset or 0)
        length0 = int(length or 0)

        attempt = 0
        bytes_sent = 0

        started_ts = time.time()
        while True:
            attempt += 1
            if attempt > self._resume_max_attempts:
                raise RuntimeError(f"S3 read_stream failed after {self._resume_max_attempts} attempts key={key} bytes_sent={bytes_sent}")

            start = start0 + bytes_sent
            remaining = 0
            if length0 > 0:
                remaining = max(0, length0 - bytes_sent)
                if remaining == 0:
                    return

            range_hdr = self._range_header(start, remaining)

            body = None
            try:
                async with self._client() as client:
                    kwargs = {"Bucket": self.bucket, "Key": key, "Range": range_hdr}
                    resp = await client.get_object(**kwargs)
                    body = resp["Body"]

                    async for chunk in body.iter_chunks(chunk_size=self._read_chunk):
                        if not chunk:
                            continue
                        b = bytes(chunk)
                        bytes_sent += len(b)
                        yield b

                # Finished without exception
                logger.info(
                    f"[S3][READ] OK key={key} bytes_sent={bytes_sent} attempts={attempt} sec={time.time()-started_ts:.2f}"
                )
                return

            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                if code in ("NoSuchKey", "404"):
                    raise FileNotFoundError(f"S3 key not found: {key}")
                if code in ("InvalidRange", "416"):
                    # treat as EOF
                    return
                logger.warning(f"[S3][READ] ClientError key={key} range={range_hdr} code={code} err={e}")
                raise

            except Exception as e:
                # This is where aiohttp ContentLengthError / connection reset etc lands.
                logger.warning(
                    f"[S3][READ] transient error key={key} range={range_hdr} bytes_sent={bytes_sent} attempt={attempt}/{self._resume_max_attempts} err={e!r}"
                )
                # backoff and retry from new Range
                await asyncio.sleep(min(self._resume_backoff_base * attempt, 10.0))
                continue

            finally:
                if body is not None:
                    try:
                        await body.close()
                    except Exception:
                        pass

    async def write_stream(self, rel_path: str, data_stream: AsyncIterator[bytes], overwrite: bool = False, append: bool = False) -> None:
        if append:
            raise NotImplementedError("Append operation is not supported on S3 driver")
        key = self._clean_key(rel_path)

        async with self._client() as client:
            if not overwrite:
                try:
                    await client.head_object(Bucket=self.bucket, Key=key)
                    raise FileExistsError(f"S3 key exists: {key}")
                except ClientError as e:
                    if e.response.get("Error", {}).get("Code") != "404":
                        raise

            upload_id = None
            try:
                mp = await client.create_multipart_upload(Bucket=self.bucket, Key=key)
                upload_id = mp["UploadId"]
                parts = []
                part_number = 1
                buffer = bytearray()

                async for chunk in data_stream:
                    if not chunk:
                        continue
                    buffer.extend(chunk)
                    if len(buffer) >= s3_cfg.CHUNK_SIZE:
                        part = await client.upload_part(
                            Bucket=self.bucket, Key=key, PartNumber=part_number,
                            UploadId=upload_id, Body=bytes(buffer)
                        )
                        parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                        part_number += 1
                        buffer = bytearray()

                if buffer:
                    part = await client.upload_part(
                        Bucket=self.bucket, Key=key, PartNumber=part_number,
                        UploadId=upload_id, Body=bytes(buffer)
                    )
                    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

                await client.complete_multipart_upload(
                    Bucket=self.bucket, Key=key, UploadId=upload_id,
                    MultipartUpload={"Parts": parts}
                )
            except Exception:
                if upload_id:
                    try:
                        await client.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload_id)
                    except Exception:
                        pass
                raise

    async def generate_presigned_url(self, rel_path: str, method: str = "GET", expiration: int = 3600) -> Optional[str]:
        key = self._clean_key(rel_path)
        client_method = "get_object" if method == "GET" else "put_object"
        try:
            async with self._client() as client:
                url = await client.generate_presigned_url(
                    ClientMethod=client_method,
                    Params={"Bucket": self.bucket, "Key": key},
                    ExpiresIn=expiration
                )
                return url
        except Exception as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            return None