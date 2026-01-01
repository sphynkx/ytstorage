import os
import time
from typing import Dict

from proto.info_pb2 import InfoRequest, InfoResponse
from proto.info_pb2_grpc import InfoServicer


class InfoService(InfoServicer):
    """
    Implementation of the Info service.
    Provides metadata for monitoring and admin dashboard.
    """

    def __init__(self, config: Dict):
        self.app_name = config.get("app_name", "YTStorage")
        self.instance_id = config.get("instance_id", os.getenv("HOSTNAME", "localhost"))
        self.host = config.get("host", "127.0.0.1:9000")
        self.version = config.get("version", "1.0.0")
        self.labels = config.get("labels", {})
        self.build_hash = config.get("build_hash", "")
        self.build_time = config.get("build_time", "")

        # Uptime tracking
        self.start_time = time.time()

    def All(self, request: InfoRequest, context) -> InfoResponse:
        """
        Returns aggregated metadata identifying the service.
        """
        uptime_seconds = int(time.time() - self.start_time)

        return InfoResponse(
            app_name=self.app_name,
            instance_id=self.instance_id,
            host=self.host,
            version=self.version,
            uptime=uptime_seconds,
            labels=self.labels,
            metrics={"uptime_sec": uptime_seconds},
            build_hash=self.build_hash,
            build_time=self.build_time,
        )