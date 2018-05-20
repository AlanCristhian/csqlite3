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
        try:
            response = self.read()
        except socket.timeout:
            raise
        if response is None:
            return
        elif response == "":
            warnings.warn("unexpected void response", RuntimeWarning)
            host, port = self.getsockname()
            _logger.warn("Unexpected void response", extra={"host": host,
                         "port": port, "pid": message[0], "kwargs": {}})
        elif isinstance(response, Warning):
            warnings.warn(response.args[1], response.__class__)
        elif isinstance(response, Exception):
            raise response
        elif isinstance(response, tuple):
            return response
        else:
            raise ValueError("Wrong response", response)


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


def connect(database, timeout=5, detect_types=False, isolation_level="",
            check_same_thread=True, factory=Connection, cached_statements=100,
            uri=False):
    return factory(database, timeout, detect_types, isolation_level,
                   check_same_thread, cached_statements, uri)
