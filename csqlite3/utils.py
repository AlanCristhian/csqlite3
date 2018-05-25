import asyncio
import collections
import configparser
import importlib
import logging
import logging.config
import pathlib
import pickle
import socket
import socketserver
import struct
import sys
import queue


BASE = pathlib.Path(__file__).parent


CONFIG = configparser.ConfigParser()
CONFIG.read(BASE/"config.ini")
HOST = CONFIG["address"]["host"]
PORT = CONFIG["address"].getint("port")
progress = {}

logging.config.fileConfig(BASE/"config.ini")
logger = logging.getLogger("Server")


Log = collections.namedtuple("Log", ["asctime", "levelname", "host", "port",
                             "status", "pid", "obj", "method", "kwargs"])


def as_log(line):
    return eval("Log(%s)" % line)


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


class ServerError:
    def __init__(self, error):
        self.error = error

    def __repr__(self):
        return f"csqlite.ServerError: {self.error}"


class ServerWarning:
    def __init__(self, warning):
        self.warning = warning

    def __repr__(self):
        return "csqlite.ServerWarning: " + repr(self.error)


class SafeLogger(queue.Queue):
    def __init__(self, name):
        super().__init__()
        self.log = logging.getLogger(name).log
        self.loop = asyncio.get_event_loop()

    def info_now(self, msg, *args, **kwargs):
        self.log(logging.INFO, msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.put_nowait((logging.CRITICAL, msg, args, kwargs))

    def error(self, msg, *args, **kwargs):
        self.put_nowait((logging.ERROR, msg, args, kwargs))

    def warning(self, msg, *args, **kwargs):
        self.put_nowait((logging.WARNING, msg, args, kwargs))

    def info(self, msg, *args, **kwargs):
        self.put_nowait((logging.INFO, msg, args, kwargs))

    def debug(self, msg, *args, **kwargs):
        self.put_nowait((logging.DEBUG, msg, args, kwargs))

    def noset(self, msg, *args, **kwargs):
        self.put_nowait((logging.NOSET, msg, args, kwargs))

    async def new_server(self):
        while True:
            if self:
                lvl, msg, args, kwargs = \
                    await self.loop.run_in_executor(None, self.get)
                self.log(lvl, msg, *args, **kwargs)


def new_progress_server(callback):
    class CallbackRequestHandler(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.recv(1)
            callback()
    return socketserver.TCPServer(("localhost", 0), CallbackRequestHandler)


def new_trace_server(trace_callback):
    class CallbackRequestHandler(socketserver.BaseRequestHandler):
        def handle(self):
            header = self.request.recv(4)
            if header:
                data = self.request.recv(*struct.unpack("!i", header))
            else:
                data = self.request.recv(0)
            trace_callback(pickle.loads(data))
    return socketserver.TCPServer(("localhost", 0), CallbackRequestHandler)


def conver_row(new_type, row):
    for item in row:
        if isinstance(item, str):
            yield new_type(item)
        else:
            yield item


def convert_str_to(new_type, iterable):
    if isinstance(iterable, list):
        return [tuple(conver_row(new_type, row)) for row in iterable]
    else:
        return tuple(conver_row(new_type, iterable))
