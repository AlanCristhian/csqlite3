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


class ModuleDispatcher(dict):
    def __init__(self, module):
        super().__init__({
            "register_converter": module.register_converter,
            "register_adapter": module.register_adapter,
            "enable_callback_tracebacks": module.enable_callback_tracebacks,
        })


class ConnectionDispatcher(dict):
    def __init__(self, host, port, pid):
        self.connection = None

        # sqlite3 module has some global variables, so I need
        # to create one sqlite3 instance per client app
        if pid in active_client_apps:
            self.sqlite3 = active_client_apps[pid]
        else:
            self.sqlite3 = active_client_apps[pid] = utils.require("sqlite3")
            extra = {"host": host, "port": port, "pid": pid,
                     "obj": "client_app", "method": "open", "arguments": {}}
            logger.debug("Client app was open.", extra=extra)
        self["open"] = self.connector

    def iterdump(self):
        iterable = self.connection.iterdump()
        def _next_iterdump():
            try:
                return next(iterable)
            except StopIteration:
                return StopIteration
        self["_next_iterdump"] = _next_iterdump
        return None

    def connector(self, **kwargs):
        self.connection = self.sqlite3.connect(**kwargs)
        self.update({
            "_get_attribute": functools.partial(getattr, self.connection),
            "_set_attribute": functools.partial(setattr, self.connection),
            "set_progress_handler": self.new_progress_handler(),
            "set_trace_callback": self.new_trace_server(),
            "iterdump": self.iterdump,
            "commit": self.connection.commit,
            "create_aggregate": self.connection.create_aggregate,
            "create_collation": self.connection.create_collation,
            "create_function": self.connection.create_function,
            "enable_load_extension": self.connection.enable_load_extension,
            "interrupt": self.connection.interrupt,
            "close": self.connection.close,
            "rollback": self.connection.rollback,
            "set_authorizer": self.connection.set_authorizer,
        })

    def new_progress_handler(self):
        def handler(address, n):
            def callable():
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect(address)
                    sock.send(b"0")
                return 0
            self.connection.set_progress_handler(callable, n)
        return handler

    def new_trace_server(self):
        def handler(address):
            def callable(data):
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect(address)
                    serialized = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)
                    header = struct.pack("!i", len(serialized))
                    sock.sendall(header + serialized)
                return 0
            self.connection.set_trace_callback(callable)
        return handler


class CursorDispatcher:
    def __init__(self, connection):
        self.connection = connection.connection
        self.cursor = None

    def connector(self, **kwargs):
        self.cursor = self.connection.cursor()

    def __getitem__(self, item):
        if item == "open":
            return self.connector
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
                        await self.handle_exception(
                            error, writer, client, host, port, *request)
                    except BaseException as error:
                        traceback.print_exc()
                        await self.handle_exception(
                            error, writer, client, host, port, *request)
                        status = StopIteration
                else:
                    status = await self.warn(writer, client, host, port)

    async def handle_exception(self, error, writer, client, host, port, pid,
                               obj, method, arguments):
        message = utils.ServerError(error)
        extra = {"host": host, "port": port, "pid": pid, "obj": obj,
                 "method": method, "arguments": arguments}
        logger.error(message, extra=extra)
        await writer(client, message)

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
