import sqlite3
import unittest

from csqlite3 import utils
from csqlite3 import server


LOG_PATH = utils.BASE.parent/"logs"/"server.log"
KEY = "127.0.0.1", 8888, "12456"


@unittest.skipIf(__name__ == "__main__",
                 "The logger was configured to clear the previous content "
                 "of the server.log file when the app start runnig.")
class ServerLoggingSuite(unittest.TestCase):
    def test_start_server(self):
        with open(LOG_PATH, "r") as log_file:
            lines = log_file.readlines()
        log = utils.as_log(lines[0])
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
        log = utils.as_log(lines[-1])
        self.assertTrue(hasattr(log, "asctime"))
        self.assertEqual(log.levelname, "INFO")
        self.assertEqual(log.host, "127.0.0.4")
        self.assertEqual(log.port, 8888)
        self.assertEqual(log.status, "csqlite3.server has been closed.")
        self.assertEqual(log.pid, "")
        self.assertEqual(log.kwargs, {})

    def test_enable_callback_traceback(self):
        with open(LOG_PATH, "r") as log_file:
            lines = log_file.readlines()
        ans = [utils.as_log(line) for line in lines]
        ans = [log for log in ans if log.obj == "csqlite3"]


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


class ModuleDispatcher(unittest.TestCase):
    def test_dispatcher_instance(self):
        database = server.Database()
        database[KEY]["csqlite3"]
        self.assertIsInstance(database[KEY]["csqlite3"],
                              server.ModuleDispatcher)

    def test_dispatcher_method(self):
        database = server.Database()
        database[KEY]["csqlite3"]
        function = database[KEY]["csqlite3"]["register_converter"]
        self.assertEqual(function.__name__, "register_converter")

    def test_enable_callback_tracebacks(self):
        database = server.Database()
        database[KEY]["csqlite3"]
        function = database[KEY]["csqlite3"]["enable_callback_tracebacks"]
        function(True)



if __name__ == '__main__':
    unittest.main()
