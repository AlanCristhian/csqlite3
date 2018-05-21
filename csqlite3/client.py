# import atexit
import logging
import os
import socket
import warnings

from . import utils


_logger = logging.getLogger("Client")
_PID = os.getpid()


class _ConnectionSocket(utils.PickleSocket):
    def request(self, *message):
        self.write(message)
        response = self.read()
        if isinstance(response, utils.ServerError):
            raise Exception(response.error)
        elif isinstance(response, utils.ServerWarning):
            warnings.warn(response.warning.args[1], response.warning.__class__)
        return response


class Cursor:
    def __init__(self, socket):
        self._socket = socket
        self._socket.request(_PID, "cursor", "open", {})

    def execute(self, sql, parameters=()):
        args = (sql, parameters)
        self._socket.request(_PID, "cursor", "execute", args)
        return self

    def fetchone(self):
        return self._socket.request(_PID, "cursor", "fetchone", {})

    def close(self):
        return self._socket.request(_PID, "cursor", "close", {})


class Connection:
    """connect(database[, timeout, detect_types, isolation_level,
               check_same_thread, cached_statements, uri])

    Opens a connection to the SQLite database file *database*. You can use
    ":memory:" to open a database connection to a database that resides in
    RAM instead of on disk."""

    def __init__(self, database, timeout=5, detect_types=False,
                 isolation_level="", check_same_thread=True,
                 cached_statements=100, uri=False):
        self._socket = _ConnectionSocket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(timeout)
        self._socket.connect((utils.HOST, utils.PORT))
        kwargs = {"database": database,
                  "timeout": timeout,
                  "detect_types": detect_types,
                  "isolation_level": isolation_level,
                  "check_same_thread": False,
                  "cached_statements": cached_statements,
                  "uri": uri}
        self._socket.request(_PID, "connection", "open", kwargs)

    def close(self):
        self._socket.request(_PID, "connection", "close", {})
        self._socket.close()

    def cursor(self):
        return Cursor(self._socket)


def connect(database, timeout=5, detect_types=False, isolation_level="",
            check_same_thread=True, factory=Connection, cached_statements=100,
            uri=False):
    return factory(database, timeout, detect_types, isolation_level,
                   check_same_thread, cached_statements, uri)


# @atexit.register
# def close_client_app():
#     with _ConnectionSocket(socket.AF_INET, socket.SOCK_STREAM) as _socket:
#         _socket.settimeout(1/200)
#         try:
#             _socket.connect((utils.HOST, utils.PORT))
#             _socket.request(_PID, "client_app", "close", {})
#         except (ConnectionRefusedError, ConnectionResetError,
#                 socket.timeout):
#             pass


def register_converter(typename, callable):
    with _ConnectionSocket(socket.AF_INET, socket.SOCK_STREAM) as _socket:
        _socket.settimeout(5)
        _socket.connect((utils.HOST, utils.PORT))
        args = (typename, callable)
        _socket.request(_PID, "csqlite3", "register_converter", args)


def register_adapter(type, callable):
    with _ConnectionSocket(socket.AF_INET, socket.SOCK_STREAM) as _socket:
        _socket.settimeout(5)
        _socket.connect((utils.HOST, utils.PORT))
        args = (type, callable)
        _socket.request(_PID, "csqlite3", "register_adapter", args)


def enable_callback_tracebacks(flag=False):
    with _ConnectionSocket(socket.AF_INET, socket.SOCK_STREAM) as _socket:
        _socket.settimeout(5)
        _socket.connect((utils.HOST, utils.PORT))
        _socket.request(_PID, "csqlite3", "enable_callback_tracebacks", [flag])

