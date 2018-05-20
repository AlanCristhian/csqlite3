import sqlite3
import unittest

from csqlite3 import utils
from csqlite3 import server


LOG_PATH = utils.BASE.parent/"logs"/"server.log"
KEY = "127.0.0.1", 8888, ":memory:"


@unittest.skipIf(__name__ == "__main__",
                 "The logger was configured to clear the previous content "
                 "of the server.log file when the app start runnig.")
class ServerLoggingSuite(unittest.TestCase):
    def test_start_server(self):
        with open(LOG_PATH, "r") as log_file:
            lines = log_file.readlines()
        log = eval("utils.Log(%s)" % lines[0])
        self.assertTrue(hasattr(log, "asctime"))
        self.assertEqual(log.levelname, "INFO")
        self.assertEqual(log.host, "127.0.0.4")
        self.assertEqual(log.port, 8888)
        self.assertEqual(log.status, "csqlite3.server has been started.")
        self.assertEqual(log.pid, "")
        self.assertEqual(log.kwargs, {})

    def test_close_server(self):
        with open(LOG_PATH, "r") as log_file:
            lines = log_file.readlines()
        log = eval("utils.Log(%s)" % lines[-1])
        self.assertTrue(hasattr(log, "asctime"))
        self.assertEqual(log.levelname, "INFO")
        self.assertEqual(log.host, "127.0.0.4")
        self.assertEqual(log.port, 8888)
        self.assertEqual(log.status, "csqlite3.server has been closed.")
        self.assertEqual(log.pid, "")
        self.assertEqual(log.kwargs, {})


class DatabaseSuite(unittest.TestCase):
    def test_database_instance(self):
        database = server.Database()
        self.assertNotIn(KEY, database)
        database[KEY]
        self.assertIn(KEY, database)


class ObjectDispatcherSuite(unittest.TestCase):
    def test_object_dispatcher_creation(self):
        database = server.Database()
        database[KEY]
        self.assertNotIn("connection", database[KEY])
        database[KEY]["connection"]
        self.assertIn("connection", database[KEY])

    def test_object_dispatcher_instance(self):
        database = server.Database()
        database[KEY]["connection"]
        self.assertIsInstance(database[KEY]["connection"],
                              server.ConnectionDispatcher)
        self.assertIsNone(database[KEY]["connection"].connection)


class ConnectionDispatcher(unittest.TestCase):
    def setUp(self):
        self.database = server.Database()
        self.database[KEY]["connection"]

    def test___init___method(self):
        self.database[KEY]["connection"]["open"](database=":memory:")
        self.assertIsInstance(self.database[KEY]["connection"].connection,
                              sqlite3.Connection)


if __name__ == '__main__':
    unittest.main()
