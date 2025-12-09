import asyncio
import logging
import grpc
from grpc_reflection.v1alpha import reflection

from proto import ytstorage_pb2, ytstorage_pb2_grpc
from server.handlers_srv import StorageServiceServicer
from drivers.driver_factory_drv import get_driver
from utils import logging_ut
from config import config

async def serve():
    logging_ut.setup_logging()
    logger = logging_ut.get_logger("server_srv")
    
    logger.info(f"Starting YtStorage v{config.VERSION}")
    logger.info(f"Driver: {config.DRIVER_KIND}")
    logger.info(f"Root FS path: {config.APP_STORAGE_FS_ROOT}")


    # Init Driver - call factory
    try:
        driver = get_driver()
        await driver.init()
    except Exception as e:
        logger.critical(f"Failed to initialize driver: {e}", exc_info=True)
        return


    # Configure Server
    max_msg_bytes = config.STORAGE_GRPC_MAX_MSG_MB * 1024 * 1024
    options = [
        ('grpc.max_send_message_length', max_msg_bytes),
        ('grpc.max_receive_message_length', max_msg_bytes),
    ]

    server = grpc.aio.server(options=options)
    
    # Register Services, create a Servicer, passing it the initialized driver
    servicer = StorageServiceServicer(driver)
    ytstorage_pb2_grpc.add_StorageServiceServicer_to_server(servicer, server)

    # Enable reflections. For test:
    # grpcurl -plaintext 127.0.0.1:50070 list
    service_names = (
        ytstorage_pb2.DESCRIPTOR.services_by_name['StorageService'].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(service_names, server)
    
    # Bind & Start
    server.add_insecure_port(config.STORAGE_REMOTE_ADDRESS)
    logger.info(f"Listening on {config.STORAGE_REMOTE_ADDRESS}")
    
    await server.start()
    
    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        logger.info("Server stopped")
        await server.stop(5)