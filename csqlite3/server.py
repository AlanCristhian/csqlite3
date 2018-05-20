import asyncio
import collections
import logging
import sqlite3

from . import utils


logger = logging.getLogger("Server")


class ConnectionDispatcher:
    def __init__(self, pid):
        self.pid = pid
        self.connection = None

    def _connector(self, **kwargs):
        self.connection = sqlite3.connect(**kwargs)

    def __getitem__(self, item):
        if item == "open":
            return self._connector
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
    def __init__(self, pid):
        self.pid = pid

    def __missing__(self, key):
        if key == "connection":
            self["connection"] = ConnectionDispatcher(self.pid)
            return self["connection"]
        elif key == "cursor":
            self["cursor"] = CursorDispatcher(self["connection"])
            return self["cursor"]
        else:
            raise KeyError


class Database(collections.defaultdict):
    def __missing__(self, key):
        self[key] = ObjectDispatcher(key[-1])
        return self[key]

    async def handler(self, reader, writer, client, host, port):
        with client:
            while "client is open":
                request = await reader(client)
                if request:
                    try:
                        pid, obj, method, kwargs = request
                        status = self[host, port, pid][obj][method](**kwargs)
                        _extra = {"host": host, "port": port,
                                  "pid": pid, "kwargs": kwargs}
                        logger.debug(status, extra=_extra)
                        await writer(client, status)
                        if obj == "connection" and method == "close":
                            break
                    except Exception as error:
                        _extra = {"host": host, "port": port,
                                  "pid": pid, "kwargs": kwargs}
                        logger.error(error, extra=_extra)
                        await writer(client, error)
                        raise

                else:
                    _extra = {"host": host, "port": port, "pid": "unknow",
                              "kwargs": {}}
                    logger.warning(
                        RuntimeWarning("Unexpected close connection"),
                        extra=_extra)
                    break


def main():
    loop = asyncio.get_event_loop()
    server = utils.new_server(utils.HOST, utils.PORT, Database().handler, loop)
    loop.create_task(server)
    _extra = {"host": utils.HOST, "port": utils.PORT, "pid": "", "kwargs": {}}
    logger.info("csqlite3.server has been started.", extra=_extra)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.close()
        logger.info("csqlite3.server has been closed.", extra=_extra)


if __name__ == '__main__':
    main()
