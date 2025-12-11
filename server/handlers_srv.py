import time
import logging
import grpc
from typing import AsyncIterator

from proto import ytstorage_pb2, ytstorage_pb2_grpc
from drivers.driver_base_drv import StorageDriver, FileStat
from utils import auth_ut, errors_ut, logging_ut
from utils.cache_ut import cache_manager
from config import config, redis_cfg

try:
    from grpc._cython.cygrpc import ExecuteBatchError
except ImportError:
    ExecuteBatchError = None

logger = logging_ut.get_logger("handlers_srv")

class StorageServiceServicer(ytstorage_pb2_grpc.StorageServiceServicer):
    """
    Implementing gRPC methods for the StorageService.
    Mapping RPC calls to driver methods with Redis caching layer.
    """

    def __init__(self, driver: StorageDriver):
        self.driver = driver

    async def _check_auth(self, context: grpc.aio.ServicerContext):
        if not auth_ut.validate_token(context.invocation_metadata()):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or missing token")

    def _map_file_type(self, is_dir: bool) -> ytstorage_pb2.FileType:
        if is_dir:
            return ytstorage_pb2.FILETYPE_DIR
        return ytstorage_pb2.FILETYPE_FILE

    def _to_stat_response(self, s: FileStat) -> ytstorage_pb2.StatResponse:
        return ytstorage_pb2.StatResponse(
            name=s.name,
            rel_path=s.rel_path,
            file_type=self._map_file_type(s.is_dir),
            size_bytes=s.size,
            created_at_ms=int(s.created_at * 1000),
            updated_at_ms=int(s.updated_at * 1000),
            etag=s.etag or ""
        )
    
    def _to_file_entry(self, s: FileStat) -> ytstorage_pb2.FileEntry:
        return ytstorage_pb2.FileEntry(
            name=s.name,
            rel_path=s.rel_path,
            file_type=self._map_file_type(s.is_dir),
            size_bytes=s.size,
            created_at_ms=int(s.created_at * 1000),
            updated_at_ms=int(s.updated_at * 1000)
        )

    # --- RPC Implementations ---

    async def Health(self, request, context):
        return ytstorage_pb2.HealthResponse(status="ok", version=config.VERSION)

    async def Stat(self, request, context):
        await self._check_auth(context)
        try:
            rel_path = request.path.rel_path
            
            # 1. Try Cache
            cached_stat = await cache_manager.get_stat(rel_path)
            if cached_stat:
                logger.debug(f"Stat Cache HIT: {rel_path}")
                return self._to_stat_response(cached_stat)

            # 2. Miss -> Driver
            logger.debug(f"Stat request (Miss): {rel_path}")
            stat_obj = await self.driver.stat(rel_path)
            
            # 3. Save to Cache
            await cache_manager.set_stat(rel_path, stat_obj)
            
            return self._to_stat_response(stat_obj)
        except Exception as e:
            code, msg = errors_ut.translate_exception(e)
            if code != grpc.StatusCode.NOT_FOUND:
                logger.warning(f"Stat warning: {msg}")
            await errors_ut.abort(context, code, msg)

    async def Exists(self, request, context):
        await self._check_auth(context)
        try:
            rel_path = request.path.rel_path
            
            # Optimization: Check cache first via Stat cache
            cached_stat = await cache_manager.get_stat(rel_path)
            if cached_stat:
                ftype = self._map_file_type(cached_stat.is_dir)
                return ytstorage_pb2.ExistsResponse(exists=True, file_type=ftype)

            exists = await self.driver.exists(rel_path)
            ftype = ytstorage_pb2.FILETYPE_UNKNOWN
            if exists:
                try:
                    stat_obj = await self.driver.stat(rel_path)
                    ftype = self._map_file_type(stat_obj.is_dir)
                    await cache_manager.set_stat(rel_path, stat_obj)
                except:
                    pass
            return ytstorage_pb2.ExistsResponse(exists=exists, file_type=ftype)
        except Exception as e:
            code, msg = errors_ut.translate_exception(e)
            await errors_ut.abort(context, code, msg)

    async def Listdir(self, request, context):
        await self._check_auth(context)
        try:
            rel_path = request.path.rel_path
            logger.debug(f"Listdir request: {rel_path}")
            items = await self.driver.listdir(rel_path)
            entries = [self._to_file_entry(x) for x in items]
            return ytstorage_pb2.ListdirResponse(entries=entries, next_page_token="")
        except Exception as e:
            code, msg = errors_ut.translate_exception(e)
            if code != grpc.StatusCode.NOT_FOUND:
                logger.warning(f"Listdir warning: {msg}")
            await errors_ut.abort(context, code, msg)

    async def Mkdirs(self, request, context):
        await self._check_auth(context)
        try:
            rel_path = request.path.rel_path
            logger.info(f"Mkdirs request: {rel_path}")
            await self.driver.mkdirs(rel_path, exist_ok=request.exist_ok)
            return ytstorage_pb2.MkdirsResponse(ok=True)
        except Exception as e:
            code, msg = errors_ut.translate_exception(e)
            logger.warning(f"Mkdirs failed: {msg}")
            await errors_ut.abort(context, code, msg)

    async def Rename(self, request, context):
        await self._check_auth(context)
        try:
            src_path = request.src.rel_path
            dst_path = request.dst.rel_path
            logger.info(f"Rename request: {src_path} -> {dst_path}")
            
            await self.driver.rename(src_path, dst_path, overwrite=request.overwrite)
            
            await cache_manager.invalidate(src_path)
            await cache_manager.invalidate(dst_path)
            
            return ytstorage_pb2.RenameResponse(ok=True)
        except Exception as e:
            code, msg = errors_ut.translate_exception(e)
            logger.warning(f"Rename failed: {msg}")
            await errors_ut.abort(context, code, msg)

    async def Remove(self, request, context):
        await self._check_auth(context)
        try:
            rel_path = request.path.rel_path
            logger.info(f"Remove request: {rel_path} (recursive={request.recursive})")
            
            await self.driver.remove(rel_path, recursive=request.recursive)
            await cache_manager.invalidate(rel_path)
            
            return ytstorage_pb2.RemoveResponse(ok=True)
        except Exception as e:
            code, msg = errors_ut.translate_exception(e)
            logger.warning(f"Remove failed: {msg}")
            await errors_ut.abort(context, code, msg)

    async def Read(self, request, context) -> AsyncIterator[ytstorage_pb2.ReadChunk]:
        await self._check_auth(context)
        try:
            rel_path = request.path.rel_path
            
            # 1. Try Cache for small files (only if reading from start)
            if request.offset == 0:
                cached_data = await cache_manager.get_file_data(rel_path)
                if cached_data:
                    logger.debug(f"Read Cache HIT: {rel_path}")
                    data_to_send = cached_data
                    if request.length > 0:
                        data_to_send = cached_data[:request.length]
                    
                    yield ytstorage_pb2.ReadChunk(data=data_to_send)
                    return

            # 2. Miss -> Prepare logic
            stat_obj = None
            cached_stat = await cache_manager.get_stat(rel_path)
            if cached_stat:
                stat_obj = cached_stat
            else:
                try:
                    stat_obj = await self.driver.stat(rel_path)
                    await cache_manager.set_stat(rel_path, stat_obj)
                except:
                    pass 

            # 3. Decide: Cache body or Direct stream
            should_cache_body = False
            if stat_obj and stat_obj.size <= redis_cfg.CACHE_MAX_FILE_SIZE and request.offset == 0:
                should_cache_body = True
                
            if should_cache_body:
                logger.debug(f"Read MISS (Small file) -> Caching: {rel_path}")
                full_data = bytearray()
                
                stream = self.driver.read_stream(rel_path, 0, 0)
                async for chunk in stream:
                    if context.done():
                        logger.debug("Read cancelled by client (cache fill)")
                        return
                    full_data.extend(chunk)
                
                await cache_manager.set_file_data(rel_path, bytes(full_data))
                
                final_data = full_data
                if request.length > 0:
                    final_data = full_data[:request.length]
                yield ytstorage_pb2.ReadChunk(data=bytes(final_data))
                
            else:
                # Direct streaming (Large file or seek)
                logger.debug(f"Read Direct: {rel_path}, offset={request.offset}")
                stream = self.driver.read_stream(rel_path, request.offset, request.length)
                
                try:
                    async for chunk_bytes in stream:
                        # Pre-check context status
                        if context.done():
                            logger.debug(f"Client disconnected early: {rel_path}")
                            break
                        yield ytstorage_pb2.ReadChunk(data=chunk_bytes)
                except Exception as e:
                    # Filter out gRPC internal errors related to disconnection
                    err_str = str(e)
                    is_exec_batch = ExecuteBatchError and isinstance(e, ExecuteBatchError)
                    
                    if is_exec_batch or "ExecuteBatchError" in err_str:
                        logger.debug(f"Client disconnected during stream (ExecuteBatchError): {rel_path}")
                    elif "Stream removed" in err_str:
                         logger.debug(f"Client disconnected during stream (Stream removed): {rel_path}")
                    else:
                        # Real IO error
                        logger.error(f"Stream error for {rel_path}: {e}")
                        raise e

        except Exception as e:
            if context.done():
                return
            
            code, msg = errors_ut.translate_exception(e)
            if code != grpc.StatusCode.NOT_FOUND:
                logger.warning(f"Read failed: {msg}")
            await errors_ut.abort(context, code, msg)

    async def Write(self, request_iterator, context) -> AsyncIterator[ytstorage_pb2.WriteAck]:
        await self._check_auth(context)
        
        class StreamState:
            bytes_count = 0

        state = StreamState()

        async def _data_extractor(iterator):
            async for envelope in iterator:
                kind = envelope.WhichOneof("kind")
                if kind == "header":
                    continue
                elif kind == "data":
                    data_len = len(envelope.data.data)
                    state.bytes_count += data_len
                    yield envelope.data.data
        
        rel_path = "unknown"
        try:
            iterator = request_iterator.__aiter__()
            try:
                first_msg = await iterator.__anext__()
            except StopAsyncIteration:
                await errors_ut.abort(context, grpc.StatusCode.INVALID_ARGUMENT, "Empty stream")

            if first_msg.WhichOneof("kind") != "header":
                await errors_ut.abort(context, grpc.StatusCode.INVALID_ARGUMENT, "First message must be WriteHeader")
            
            header = first_msg.header
            rel_path = header.path.rel_path
            logger.info(f"Start Write: {rel_path}, ovr={header.overwrite}, app={header.append}")

            await self.driver.write_stream(
                rel_path=rel_path,
                data_stream=_data_extractor(iterator),
                overwrite=header.overwrite,
                append=header.append
            )

            await cache_manager.invalidate(rel_path)
            yield ytstorage_pb2.WriteAck(ok=True, bytes_written=state.bytes_count)

        except Exception as e:
            code, msg = errors_ut.translate_exception(e)
            logger.warning(f"Write failed: {msg}")
            await cache_manager.invalidate(rel_path)
            yield ytstorage_pb2.WriteAck(ok=False, error=str(e), bytes_written=state.bytes_count)

    # --- Direct Access ---
    async def GeneratePresignedUrl(self, request, context):
        await self._check_auth(context)
        try:
            rel_path = request.path.rel_path
            method_str = "PUT" if request.method == ytstorage_pb2.URL_METHOD_PUT else "GET"
            
            logger.debug(f"Url gen request: {rel_path} ({method_str})")
            
            url = await self.driver.generate_presigned_url(
                rel_path=rel_path, 
                method=method_str, 
                expiration=request.expiration_seconds
            )
            
            supported = (url is not None)
            expires_at = int((time.time() + request.expiration_seconds) * 1000) if supported else 0
            
            return ytstorage_pb2.GenerateUrlResponse(
                supported=supported,
                url=url or "",
                expires_at_ms=expires_at
            )
        except Exception as e:
             code, msg = errors_ut.translate_exception(e)
             logger.warning(f"GenerateUrl failed: {msg}")
             await errors_ut.abort(context, code, msg)


    # --- Job stubs ---
    async def EnqueuePut(self, request, context):
        await self._check_auth(context)
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, "Jobs not implemented in MVP")

    async def EnqueueGet(self, request, context):
        await self._check_auth(context)
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, "Jobs not implemented in MVP")

    async def JobStatus(self, request, context):
        await self._check_auth(context)
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, "Jobs not implemented in MVP")

    async def CancelJob(self, request, context):
        await self._check_auth(context)
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, "Jobs not implemented in MVP")