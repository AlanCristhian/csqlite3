import atexit
import pickle
import struct
import logging
import os
import socket
import threading
import warnings
import pathlib
import sys

# Fix cProfile running
from os.path import dirname
sys.path.append(dirname(__file__))

try:
    import utils
except ImportError:
    from . import utils


_logger = logging.getLogger("Client")
_PID = os.getpid()


class _PickleSocket(socket.socket):
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


class _ConnectionSocket(_PickleSocket):
    def request(self, *message):
        self.write(message)
        response = self.read()
        if isinstance(response, utils.ServerError):
            raise response.error
        elif isinstance(response, utils.ServerWarning):
            warnings.warn(response.warning.args[1], response.warning.__class__)
        return response


class Cursor:
    """SQLite database cursor class."""
    def __init__(self, database, socket, row_factory, text_factory):
        self._socket = socket
        self._request = self._socket.request
        self._row_factory = row_factory
        self._text_factory = text_factory
        self._database = database
        self._request(_PID, "cursor", "open", {})

    def execute(self, sql, parameters=()):
        """Executes a SQL statement."""
        self._request(_PID, "cursor", "execute", [sql, parameters])
        return self

    def fetchone(self):
        """Fetches one row from the resultset."""
        data = self._request(_PID, "cursor", "fetchone", {})
        if self._row_factory:
            data = self._row_factory(self, data)
        return data

    def fetchall(self):
        """Fetches all rows from the resultset."""
        data = self._request(_PID, "cursor", "fetchall", ())
        if self._row_factory:
            if isinstance(data, list):
                for i, row in enumerate(data):
                    data[i] = self._row_factory(self, row)
        return data

    def fetchmany(self, size=None):
        """Repeatedly executes a SQL statement."""
        if size is None:
            size = self.arraysize
        return self._request(_PID, "cursor", "fetchmany", [size])

    def close(self):
        """Closes the cursor."""
        return self._request(_PID, "cursor", "close", {})

    @property
    def rowcount(self):
        return self._request(_PID, "cursor", "_get_attribute", ["rowcount"])

    @property
    def lastrowid(self):
        return self._request(_PID, "cursor", "_get_attribute", ["lastrowid"])

    @property
    def arraysize(self):
        return self._request(_PID, "cursor", "_get_attribute", ["arraysize"])

    @arraysize.setter
    def arraysize(self, size):
        return self._request(_PID, "cursor", "_set_attribute",
                             ["arraysize", size])

    def __iter__(self):
        """Implement iter(self)."""
        return iter(self.fetchall())

    def executemany(self, sql, seq_of_parameters):
        """Repeatedly executes a SQL statement."""
        self._request(_PID, "cursor", "executemany", [sql, seq_of_parameters])
        return self

    def executescript(self, sql_script):
        """Executes a multiple SQL statements at once."""
        self._request(_PID, "cursor", "executescript", [sql_script])
        return self

    @property
    def description(self):
        return self._request(_PID, "cursor", "_get_attribute", ["description"])


class Connection:
    """connect(database[, timeout, detect_types, isolation_level,
               check_same_thread, cached_statements, uri])

    Opens a connection to the SQLite database file *database*. You can use
    ":memory:" to open a database connection to a database that resides in
    RAM instead of on disk."""

    def __init__(self, database, timeout=5, detect_types=False,
                 isolation_level="", check_same_thread=True,
                 cached_statements=100, uri=False):
        self.isolation_level = isolation_level
        self._socket = _ConnectionSocket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(timeout)
        self._socket.connect((utils.HOST, utils.PORT))
        self._cursor = None
        self._progress = None
        self._trace = None
        self._row_factory = None
        self._text_factory = None
        if ":memory:" in database:
            self._database = database
        else:
            self._database = str(pathlib.Path(database).resolve())
        kwargs = {"database": database,
                  "timeout": timeout,
                  "detect_types": detect_types,
                  "isolation_level": isolation_level,
                  "check_same_thread": False,
                  "cached_statements": cached_statements,
                  "uri": uri}
        self._request = self._socket.request
        self._request(_PID, "connection", "open", kwargs)

    @property
    def in_transaction(self):
        return self._request(_PID, "connection", "_get_attribute",
                             ["in_transaction"])

    def cursor(self, factory=Cursor):
        """Return a cursor for the connection."""
        if not self._cursor:
            self._cursor = factory(self._database, self._socket,
                                   self._row_factory, self._text_factory)
        return self._cursor

    def commit(self):
        """Commit the current transaction."""
        return self._request(_PID, "connection", "commit", ())

    def rollback(self):
        """Roll back the current transaction."""
        return self._request(_PID, "connection", "rollback", ())

    def close(self):
        """Closes the connection."""
        self._request(_PID, "connection", "close", {})
        self._socket.close()
        if self._progress:
            self._progress.shutdown()

    def execute(self, sql, parameters=()):
        """Executes a SQL statement. Non-standard."""
        return self.cursor().execute(sql, parameters)

    def executemany(self, sql, seq_of_parameters):
        """Repeatedly executes a SQL statement. Non-standard."""
        return self.cursor().executemany(sql, seq_of_parameters)

    def executescript(self, sql_script):
        """Executes a multiple SQL statements at once. Non-standard."""
        return self.cursor().executescript(sql_script)

    def create_function(self, name, num_params, func):
        """Creates a new function. Non-standard."""
        return self._request(_PID, "connection", "create_function",
                             [name, num_params, func])

    def create_aggregate(self, name, num_params, aggregate_class):
        """Creates a new aggregate. Non-standard."""
        return self._request(_PID, "connection", "create_aggregate",
                             [name, num_params, aggregate_class])

    def create_collation(self, name, callable):
        """Creates a collation function. Non-standard."""
        return self._request(_PID, "connection", "create_collation",
                             [name, callable])

    def interrupt(self):
        """Abort any pending database operation. Non-standard."""
        return self._request(_PID, "connection", "interrupt", ())

    def set_authorizer(self, authorizer_callback):
        """Sets authorizer callback. Non-standard."""
        return self._request(_PID, "connection", "set_authorizer",
                             [authorizer_callback])

    def set_progress_handler(self, handler, n):
        """Sets progress handler callback. Non-standard."""
        if not self._progress:
            self._progress = utils.new_progress_server(handler)
            thread = threading.Thread(target=self._progress.serve_forever)
            thread.daemon = True
            thread.start()
        arguments = (self._progress.server_address, n)
        return self._request(_PID, "connection", "set_progress_handler",
                             arguments)

    def set_trace_callback(self, trace_callback):
        """Sets a trace callback called for each SQL
        statement (passed as unicode). Non-standard.
        """
        if not self._trace:
            self._trace = utils.new_trace_server(trace_callback)
            thread = threading.Thread(target=self._trace.serve_forever)
            thread.daemon = True
            thread.start()
        return self._request(_PID, "connection", "set_trace_callback",
                             [self._trace.server_address])

    def enable_load_extension(self, enabled):
        """Enable dynamic loading of SQLite
        extension modules. Non-standard.
        """
        return self._request(_PID, "connection", "enable_load_extension",
                             [enabled])

    def load_extension(self, path):
        """Load SQLite extension module. Non-standard."""
        return self._request(_PID, "connection", "load_extension", [path])

    @property
    def row_factory(self):
        return self._row_factory

    @row_factory.setter
    def row_factory(self, factory):
        self._request(_PID, "connection", "_set_attribute",
                      ["row_factory", factory])
        self._row_factory = factory
        if self._cursor:
            self._cursor._row_factory = factory

    @property
    def text_factory(self):
        return self._text_factory

    @text_factory.setter
    def text_factory(self, factory):
        self._request(_PID, "connection", "_set_attribute",
                      ["text_factory", factory])
        self._text_factory = factory

    @property
    def total_changes(self):
        return self._request(_PID, "connection", "_get_attribute",
                             ["total_changes"])

    def iterdump(self):
        self._request(_PID, "connection", "iterdump", ())

        def iter_dump():
            while True:
                row = self._request(_PID, "connection", "_next_iterdump", ())
                if row is StopIteration:
                    break
                else:
                    yield row

        return iter_dump()


def connect(database, timeout=5, detect_types=False, isolation_level="",
            check_same_thread=True, factory=Connection, cached_statements=100,
            uri=False):
    """connect(database[, timeout, detect_types, isolation_level,
               check_same_thread, factory, cached_statements, uri])

    Opens a connection to the SQLite database file *database*. You can use
    ":memory:" to open a database connection to a database that resides in
    RAM instead of on disk."""

    return factory(database, timeout, detect_types, isolation_level,
                   check_same_thread, cached_statements, uri)


@atexit.register
def close_client_app():
    with _ConnectionSocket(socket.AF_INET, socket.SOCK_STREAM) as _socket:
        _socket.settimeout(1/200)
        try:
            _socket.connect((utils.HOST, utils.PORT))
            _socket.request(_PID, "client_app", "close", {})
        except (ConnectionRefusedError, ConnectionResetError,
                socket.timeout):
            pass


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
