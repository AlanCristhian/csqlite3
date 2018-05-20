import builtins
import collections
import configparser
import importlib
import io
import logging
import logging.config
import pathlib
import pickle
import socket
import sqlite3
import struct
import sys


BASE = pathlib.Path(__file__).parent


CONFIG = configparser.ConfigParser()
CONFIG.read(BASE/"config.ini")
HOST = CONFIG["address"]["host"]
PORT = CONFIG["address"].getint("port")


logging.config.fileConfig(BASE/"config.ini")
logger = logging.getLogger("Server")


Log = collections.namedtuple("Log", [
    "asctime",
    "levelname",
    "host",
    "port",
    "status",
    "pid",
    "kwargs",
])


def log_file(path):
    with open(BASE.parent/"logs"/path, "r") as log_file:
        for line in log_file:
            yield eval("Log(%s)" % line)


class PickleSocket(socket.socket):
    def write(self, message):
        data = pickle.dumps(message, pickle.HIGHEST_PROTOCOL)
        size = len(data)
        header = struct.pack("!i", size)
        self.sendall(header + data)

    def read(self):
        header = self.recv(4)
        if header:
            size = struct.unpack("!i", header)[0]
            return pickle.loads(self.recv(size))
        else:
            # It is safe because unpickle the `b''` constant
            return pickle.loads(self.recv(0))

    def close(self):
        super().close()


async def new_server(host, port, handler, loop):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(5)
    sock.setblocking(False)

    async def reader(sock):
        header = await loop.sock_recv(sock, 4)
        if header:
            data = await loop.sock_recv(sock, *struct.unpack("!i", header))
        else:
            data = await loop.sock_recv(sock, 0)
        return pickle.loads(data)

    async def writer(sock, data):
        serialized = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)
        header = struct.pack("!i", len(serialized))
        await loop.sock_sendall(sock,  header + serialized)

    with sock:
        while True:
            client, (host, port) = await loop.sock_accept(sock)
            loop.create_task(handler(reader, writer, client, host, port))


def require(name):
    """Make an non-singleton instance of a module."""
    module = importlib.import_module(name)
    if name in sys.modules:
        del sys.modules[name]
    return module
