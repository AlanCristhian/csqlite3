import asyncio
import collections
import functools
import pickle
import socket
import sqlite3
import struct
import traceback


from . import utils


logger = utils.SafeLogger("Server")
active_client_apps = {}

SQLITE3_EXCEPTIONS = (sqlite3.Warning, sqlite3.DataError,
                      sqlite3.DatabaseError, sqlite3.Error,
                      sqlite3.IntegrityError, sqlite3.InterfaceError,
                      sqlite3.InternalError, sqlite3.NotSupportedError,
                      sqlite3.OperationalError, sqlite3.ProgrammingError)


def dummy_function():
    pass


class ModuleDispatcher:
    def __init__(self, module):
        self.module = module

    def __getitem__(self, method):
        return getattr(self.module, method)


def new_progress_handler(connection):
    def handler(address, n):
        def callable():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(address)
                sock.send(b"0")
            return 0
        connection.set_progress_handler(callable, n)
    return handler


def new_trace_server(connection):
    def handler(address):
        def callable(data):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(address)
                serialized = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)
                header = struct.pack("!i", len(serialized))
                sock.sendall(header + serialized)
            return 0
        connection.set_trace_callback(callable)
    return handler


class ConnectionDispatcher:
    def __init__(self, host, port, pid):
        self.connection = None
        self._iter_iterdump = None

        # sqlite3 module has some global variables, so I need
        # to create one sqlite3 instance per client app
        if pid in active_client_apps:
            self.sqlite3 = active_client_apps[pid]
        else:
            self.sqlite3 = active_client_apps[pid] = utils.require("sqlite3")
            extra = {"host": host, "port": port, "pid": pid,
                     "obj": "client_app", "method": "open", "arguments": {}}
            logger.debug("Client app was open.", extra=extra)

    def _connector(self, **kwargs):
        self.connection = self.sqlite3.connect(**kwargs)

    def __getitem__(self, method):
        if method == "open":
            return self._connector
        if method == "_get_attribute":
            return functools.partial(getattr, self.connection)
        elif method == "_set_attribute":
            return functools.partial(setattr, self.connection)
        elif method == "set_progress_handler":
            return new_progress_handler(self.connection)
        elif method == "set_trace_callback":
            return new_trace_server(self.connection)
        elif method == "iterdump":
            iterable = self.connection.iterdump()

            def _next_iterdump():
                try:
                    return next(iterable)
                except StopIteration:
                    return StopIteration

            self._next_iterdump = _next_iterdump
            return dummy_function
        elif method == "_next_iterdump":
            return self._next_iterdump
        return getattr(self.connection, method)


class CursorDispatcher:
    def __init__(self, connection):
        self.connection = connection.connection
        self.cursor = None

    def _connector(self, **kwargs):
        self.cursor = self.connection.cursor()

    def __getitem__(self, item):
        if item == "open":
            return self._connector
        elif item == "_get_attribute":
            return functools.partial(getattr, self.cursor)
        return getattr(self.cursor, item)


class ObjectDispatcher(collections.defaultdict):
    def __init__(self, key):
        self.key = key

    def __missing__(self, key):
        if key == "connection":
            self["connection"] = ConnectionDispatcher(*self.key)
            return self["connection"]
        elif key == "cursor":
            self["cursor"] = CursorDispatcher(self["connection"])
            return self["cursor"]
        elif key == "csqlite3":
            self["csqlite3"] = ModuleDispatcher(
                active_client_apps[self.key[2]])
            return self["csqlite3"]
        else:
            raise KeyError


class Database(collections.defaultdict):
    def __missing__(self, key):
        self[key] = ObjectDispatcher(key)
        return self[key]

    async def handler(self, reader, writer, client, host, port):
        with client:
            status = None
            while status is not StopIteration:
                request = await reader(client)
                if request:
                    try:
                        status = await self.handle_request(
                            writer, client, host, port, *request)
                    except SQLITE3_EXCEPTIONS as error:
                        pid, obj, method, arguments = request
                        message = utils.ServerError(error)
                        extra = {"host": host, "port": port, "pid": pid,
                                 "obj": obj, "method": method,
                                 "arguments": arguments}
                        logger.error(message, extra=extra)
                        await writer(client, message)
                        # traceback.print_exc()
                    except BaseException as error:
                        pid, obj, method, arguments = request
                        message = utils.ServerError(error)
                        extra = {"host": host, "port": port, "pid": pid,
                                 "obj": obj, "method": method,
                                 "arguments": arguments}
                        logger.error(message, extra=extra)
                        await writer(client, message)
                        traceback.print_exc()
                        break
                else:
                    status = await self.warn(writer, client, host, port)

    async def handle_request(self, writer, client, host, port, pid, obj,
                             method, arguments):
        if (obj == "close") and (method == "client_app"):
            del active_client_apps[pid]
            extra = {"host": host, "port": port, "pid": pid, "obj": obj,
                     "method": method, "arguments": arguments}
            logger.debug("Client app was closed.", extra=extra)
            return StopIteration
        if isinstance(arguments, dict):
            message = self[host, port, pid][obj][method](**arguments)
        else:
            message = self[host, port, pid][obj][method](*arguments)
        if isinstance(message, sqlite3.Cursor):
            message = None
        logger.debug(message, extra={"host": host, "port": port, "pid": pid,
                                     "obj": obj, "method": method,
                                     "arguments": repr(arguments)})
        await writer(client, message)
        if (obj == "close") and (method == "connection"):
            return StopIteration

    async def warn(self, writer, client, host, port):
        warning = RuntimeWarning("Unexpected close connection")
        message = utils.ServerWarning(warning)
        logger.warning(message, extra={"host": host, "port": port, "obj": "",
                       "method": "", "pid": "", "arguments": {}})
        await writer(client, message)
        return StopIteration


async def new_monitor():
    import psutil
    import pathlib
    bench = pathlib.Path("bench")
    with open(bench/"cpu_percent.csv", "w", newline="") as cpu_file, \
         open(bench/"memory_usage.csv", "w", newline="") as memory_file:
        while True:
            cpu_writer = csv.writer(cpu_file)
            memory_writer = csv.writer(memory_file)

            cpu_percent = psutil.cpu_percent(interval=0.1, percpu=True)
            memory_usage = [psutil.virtual_memory().used]

            cpu_writer.writerow(cpu_percent)
            memory_writer.writerow(memory_usage)

            cpu_file.flush()
            memory_file.flush()

            await asyncio.sleep(0.2)


def main():
    loop = asyncio.get_event_loop()
    handler = Database().handler
    database_server = utils.new_server(utils.HOST, utils.PORT, handler, loop)
    logging_server = logger.new_server()
    # monitor = new_monitor()
    _extra = {"host": utils.HOST, "port": utils.PORT, "pid": "",
              "obj": "", "method": "", "arguments": {}}
    logger.info("csqlite3.server has been started.", extra=_extra)
    try:
        # tasks = asyncio.gather(database_server, logging_server, monitor)
        tasks = asyncio.gather(database_server, logging_server)
        loop.run_until_complete(tasks)
    except KeyboardInterrupt:
        logger.info_now("csqlite3.server has been closed.", extra=_extra)
        loop.stop()
    finally:
        loop.close()


if __name__ == '__main__':
    main()
