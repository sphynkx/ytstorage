import asyncio
import logging
import grpc
from grpc_reflection.v1alpha import reflection
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from proto import ytstorage_pb2, ytstorage_pb2_grpc, info_pb2_grpc
from server.handlers_srv import StorageServiceServicer
from services.info_srv import InfoService
from drivers.driver_factory_drv import get_driver
from utils import logging_ut
from config import config

async def serve():
    """
    Starts the gRPC server, registers services, and enables reflection.
    """
    # Setup logging
    logging_ut.setup_logging()
    logger = logging_ut.get_logger("server_srv")
    
    logger.info(f"Starting YtStorage v{config.VERSION}")
    logger.info(f"Driver: {config.DRIVER_KIND}")
    logger.info(f"Root FS path: {config.APP_STORAGE_FS_ROOT}")

    # Initialize driver via factory
    try:
        driver = get_driver()
        await driver.init()
    except Exception as e:
        logger.critical(f"Failed to initialize driver: {e}", exc_info=True)
        return

    # Configure gRPC server
    max_msg_bytes = config.STORAGE_GRPC_MAX_MSG_MB * 1024 * 1024
    options = [
        ('grpc.max_send_message_length', max_msg_bytes),
        ('grpc.max_receive_message_length', max_msg_bytes),
    ]

    server = grpc.aio.server(options=options)
    
    # Register Storage Service
    servicer = StorageServiceServicer(driver)
    ytstorage_pb2_grpc.add_StorageServiceServicer_to_server(servicer, server)

    # Register Health Check Service
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    # Set general service health status to SERVING
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

    # Register Info Service
    app_config = {
        "app_name": "YurTube Storage Service",
        "instance_id": "VegaVM",
        "host": config.STORAGE_REMOTE_ADDRESS,
        "version": config.VERSION,
        "labels": {"env": "prod", "region": "eu"}
    }
    info_servicer = InfoService(app_config)
    info_pb2_grpc.add_InfoServicer_to_server(info_servicer, server)

    # Enable Server Reflection
    service_names = (
        ytstorage_pb2.DESCRIPTOR.services_by_name['StorageService'].full_name,
        health.SERVICE_NAME,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(service_names, server)
    
    # Bind and start the gRPC server
    server.add_insecure_port(config.STORAGE_REMOTE_ADDRESS)
    logger.info(f"Listening on {config.STORAGE_REMOTE_ADDRESS}")
    
    # Start the server and wait for termination
    await server.start()
    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        logger.info("Server shutdown initiated")
        await server.stop(5)