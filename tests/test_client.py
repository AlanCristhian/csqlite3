import unittest
import inspect

from csqlite3 import client


class ConnectionSuite(unittest.TestCase):
    def setUp(self):
        self.connection = client.Connection("test_connection.db")

    def tearDown(self):
        self.connection.close()

    def test_connection_instance(self):
        self.assertIsInstance(self.connection, client.Connection)


class FunctionsSuite(unittest.TestCase):
    def test_connect_function(self):
        signature = inspect.signature(client.connect)
        self.assertEqual(signature.parameters["factory"].default,
                         client.Connection)

    def test_connect_function_execution(self):
        connection = client.connect(":memory:")
        self.assertIsInstance(connection, client.Connection)


if __name__ == '__main__':
    unittest.main()
