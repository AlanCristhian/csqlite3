import asyncio
import collections

from . import utils


logger = utils.SafeLogger("Server")
active_client_apps = {}


class ConnectionDispatcher:
    def __init__(self, host, port, pid):
        self.connection = None

        # sqlite3 module has some global variables, so I need
        # to create one sqlite3 instance per client app
        if pid in active_client_apps:
            self.sqlite3 = active_client_apps[pid]
        else:
            self.sqlite3 = active_client_apps[pid] = utils.require("sqlite3")
            logger.debug("Client app was open.", extra={"host": host,
                         "port": port, "pid": pid, "kwargs": {}})

    def connector(self, **kwargs):
        self.connection = self.sqlite3.connect(**kwargs)

    def __getitem__(self, item):
        if item == "open":
            return self.connector
        return getattr(self.connection, item)


class CursorDispatcher:
    def __init__(self, connection):
        self.connection = connection

    def open(self, **kwargs):
        self.cursor = self.connection.cursor()
        return self

    def __getitem__(self, item):
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
                    except Exception as error:
                        pid, *_, kwargs = request
                        status = utils.ServerError(error)
                        logger.error(status, extra={"host": host, "port": port,
                                     "pid": pid, "kwargs": kwargs})
                        await writer(client, status)
                        raise

                else:
                    status = await self.warn(writer, client, host, port)

    async def handle_request(self, writer, client, host, port, pid, obj,
                             method, kwargs):
        status = self[host, port, pid][obj][method](**kwargs)
        logger.debug(status, extra={"host": host, "port": port, "pid": pid,
                     "kwargs": kwargs})

        await writer(client, status)

        if (obj, method) == ("connection", "close"):
            return StopIteration
        if (obj, method) == ("client_app", "close"):
            del active_client_apps[pid]
            logger.debug("Client app was closed.", extra={"host": host,
                         "port": port, "pid": pid, "kwargs": kwargs})
            return StopIteration

    async def warn(self, writer, client, host, port):
        warning = RuntimeWarning("Unexpected close connection")
        status = utils.ServerWarning(warning)
        logger.warning(status, extra={"host": host, "port": port,
                       "pid": "unknow", "kwargs": {}})
        await writer(client, status)
        return StopIteration


def main():
    loop = asyncio.get_event_loop()
    handler = Database().handler
    database_server = utils.new_server(utils.HOST, utils.PORT, handler, loop)
    logging_server = logger.new_server()
    _extra = {"host": utils.HOST, "port": utils.PORT, "pid": "", "kwargs": {}}
    logger.info("csqlite3.server has been started.", extra=_extra)
    try:
        tasks = asyncio.gather(database_server, logging_server)
        loop.run_until_complete(tasks)
    except KeyboardInterrupt:
        logger.info_now("csqlite3.server has been closed.", extra=_extra)
        loop.stop()
    finally:
        loop.close()


if __name__ == '__main__':
    main()
