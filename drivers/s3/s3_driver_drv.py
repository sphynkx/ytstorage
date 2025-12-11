import asyncio
import os
import time
from typing import AsyncIterator, List, Optional
from contextlib import asynccontextmanager

from aiobotocore.session import get_session
from botocore.exceptions import ClientError

from drivers.driver_base_drv import StorageDriver, FileStat
from config import s3_cfg
from utils import logging_ut, errors_ut

logger = logging_ut.get_logger("s3_driver")

class S3Driver(StorageDriver):
    """
    S3-compatible driver (Ceph, MinIO, AWS).
    Uses aiobotocore for async operations.
    """

    def __init__(self):
        self.endpoint = s3_cfg.S3_ENDPOINT_URL
        self.access_key = s3_cfg.S3_ACCESS_KEY_ID
        self.secret_key = s3_cfg.S3_SECRET_ACCESS_KEY
        self.bucket = s3_cfg.S3_BUCKET_NAME
        self.region = s3_cfg.S3_REGION_NAME
        self.session = get_session()

    @asynccontextmanager
    async def _client(self):
        """Context manager for S3 client."""
        async with self.session.create_client(
            's3',
            region_name=self.region,
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        ) as client:
            yield client

    def _clean_key(self, rel_path: str) -> str:
        """S3 keys shouldn't start with /"""
        return rel_path.strip("/")

    async def init(self) -> None:
        """Check connection and bucket existence."""
        logger.info(f"Initializing S3 Driver: {self.endpoint} / {self.bucket}")
        try:
            async with self._client() as client:
                await client.head_bucket(Bucket=self.bucket)
                logger.info("S3 Bucket exists.")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404":
                logger.warning(f"Bucket {self.bucket} not found. Trying to create...")
                try:
                    async with self._client() as client:
                        await client.create_bucket(Bucket=self.bucket)
                        logger.info("Bucket created successfully.")
                except Exception as create_err:
                    raise ConnectionError(f"Failed to create bucket: {create_err}")
            elif error_code == "403":
                raise PermissionError(f"Access denied to bucket {self.bucket}")
            else:
                raise ConnectionError(f"S3 Connection failed: {e}")

    async def stat(self, rel_path: str) -> FileStat:
        key = self._clean_key(rel_path)
        try:
            async with self._client() as client:
                response = await client.head_object(Bucket=self.bucket, Key=key)
                
                return FileStat(
                    name=os.path.basename(key),
                    rel_path=rel_path,
                    is_dir=False,
                    size=response['ContentLength'],
                    created_at=response.get('LastModified').timestamp(),
                    updated_at=response.get('LastModified').timestamp(),
                    etag=response.get('ETag', '').strip('"')
                )
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                if await self._is_dir(key):
                     return FileStat(
                        name=os.path.basename(key.rstrip('/')),
                        rel_path=rel_path,
                        is_dir=True,
                        size=0,
                        created_at=0,
                        updated_at=0
                    )
                raise FileNotFoundError(f"S3 Object not found: {key}")
            raise e

    async def _is_dir(self, prefix: str) -> bool:
        if not prefix.endswith('/'):
            prefix += '/'
        async with self._client() as client:
            res = await client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
            return 'Contents' in res or 'CommonPrefixes' in res

    async def exists(self, rel_path: str) -> bool:
        try:
            await self.stat(rel_path)
            return True
        except FileNotFoundError:
            return False

    async def listdir(self, rel_path: str) -> List[FileStat]:
        prefix = self._clean_key(rel_path)
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        
        results = []
        async with self._client() as client:
            paginator = client.get_paginator('list_objects_v2')
            async for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter='/'):
                for p in page.get('CommonPrefixes', []):
                    dir_name = p['Prefix'][len(prefix):].strip('/')
                    results.append(FileStat(
                        name=dir_name,
                        rel_path=p['Prefix'].strip('/'),
                        is_dir=True,
                        size=0,
                        created_at=0,
                        updated_at=0
                    ))
                for c in page.get('Contents', []):
                    key = c['Key']
                    if key == prefix: continue
                    name = key[len(prefix):]
                    results.append(FileStat(
                        name=name,
                        rel_path=key,
                        is_dir=False,
                        size=c['Size'],
                        created_at=c['LastModified'].timestamp(),
                        updated_at=c['LastModified'].timestamp(),
                        etag=c.get('ETag', '').strip('"')
                    ))
        return results

    async def mkdirs(self, rel_path: str, exist_ok: bool = False) -> None:
        key = self._clean_key(rel_path)
        if not key.endswith('/'):
            key += '/'
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
                    if e.response['Error']['Code'] != "404": raise e
            
            copy_source = {'Bucket': self.bucket, 'Key': src_key}
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
                if not key.endswith('/'): key += '/'
                paginator = client.get_paginator('list_objects_v2')
                async for page in paginator.paginate(Bucket=self.bucket, Prefix=key):
                    if 'Contents' in page:
                        objects = [{'Key': obj['Key']} for obj in page['Contents']]
                        await client.delete_objects(Bucket=self.bucket, Delete={'Objects': objects})
            else:
                 if await self._is_dir(key):
                     raise OSError(39, "Directory not empty (S3 prefix)")

    # --- CRITICAL FIX: Safe Stream Reading ---
    async def read_stream(self, rel_path: str, offset: int = 0, length: int = 0) -> AsyncIterator[bytes]:
        key = self._clean_key(rel_path)
        
        # Manually manage session for granular control over the stream body
        session = get_session()
        async with session.create_client(
            's3',
            region_name=self.region,
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        ) as client:
            
            kwargs = {'Bucket': self.bucket, 'Key': key}
            if offset > 0 or length > 0:
                end = ""
                if length > 0:
                    end = offset + length - 1
                kwargs['Range'] = f"bytes={offset}-{end}"

            body = None
            try:
                response = await client.get_object(**kwargs)
                body = response['Body']
                
                # Iterate chunks safely
                async for chunk in body.iter_chunks(chunk_size=s3_cfg.CHUNK_SIZE):
                    yield chunk

            except ClientError as e:
                if e.response['Error']['Code'] == "NoSuchKey":
                    raise FileNotFoundError(f"S3 key not found: {key}")
                # 416 Range Not Satisfiable might happen on race conditions
                if e.response['Error']['Code'] == "InvalidRange":
                     # Return empty stream instead of crash if range is bad
                     return 
                raise e
            finally:
                # Always close the underlying stream to prevent "Unclosed connection" logs
                if body:
                    body.close()


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
                    if e.response['Error']['Code'] != "404": raise e
            
            upload_id = None
            try:
                mp = await client.create_multipart_upload(Bucket=self.bucket, Key=key)
                upload_id = mp['UploadId']
                parts = []
                part_number = 1
                buffer = bytearray()
                
                async for chunk in data_stream:
                    buffer.extend(chunk)
                    if len(buffer) >= s3_cfg.CHUNK_SIZE:
                        part = await client.upload_part(
                            Bucket=self.bucket, Key=key, PartNumber=part_number,
                            UploadId=upload_id, Body=buffer
                        )
                        parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                        part_number += 1
                        buffer = bytearray()

                if buffer:
                    part = await client.upload_part(
                        Bucket=self.bucket, Key=key, PartNumber=part_number,
                        UploadId=upload_id, Body=buffer
                    )
                    parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                
                await client.complete_multipart_upload(
                    Bucket=self.bucket, Key=key, UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
            except Exception as e:
                if upload_id:
                    await client.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload_id)
                raise e

    async def generate_presigned_url(self, rel_path: str, method: str = "GET", expiration: int = 3600) -> Optional[str]:
        key = self._clean_key(rel_path)
        client_method = 'get_object' if method == "GET" else 'put_object'
        try:
            async with self._client() as client:
                url = await client.generate_presigned_url(
                    ClientMethod=client_method,
                    Params={'Bucket': self.bucket, 'Key': key},
                    ExpiresIn=expiration
                )
                return url
        except Exception as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            return None