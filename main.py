import asyncio
import sys

from server.server_srv import serve

if __name__ == '__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass