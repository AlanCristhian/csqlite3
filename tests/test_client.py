import inspect
import unittest

import csqlite3


class Point:
    def __init__(self, x, y):
        self.x, self.y = x, y

    def __repr__(self):
        return "(%f;%f)" % (self.x, self.y)

def adapt_point(point):
    return ("%f;%f" % (point.x, point.y)).encode('ascii')

def convert_point(s):
    x, y = list(map(float, s.split(b";")))
    return Point(x, y)


class CursorSuite(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.connection = csqlite3.connect(":memory:")
        self.cursor = self.connection.cursor()

    @classmethod
    def tearDownClass(self):
        self.cursor.close()
        self.connection.close()

    def test_cursor_instance(self):
        self.assertIsInstance(self.cursor, csqlite3.Cursor)

    def test_execute_and_fetchone(self):
        self.cursor.execute("CREATE TABLE cursor (p INT)")
        self.cursor.execute("INSERT INTO cursor (p) VALUES (1)")
        self.cursor.execute("SELECT p FROM cursor WHERE p=1")
        obtained = self.cursor.fetchone()[0]
        self.assertEqual(obtained, 1)


class ConnectionSuite(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.connection = csqlite3.Connection(":memory:")

    @classmethod
    def tearDownClass(self):
        self.connection.close()

    def test_connection_instance(self):
        self.assertIsInstance(self.connection, csqlite3.Connection)


class FunctionsSuite(unittest.TestCase):
    def test_connect_function(self):
        signature = inspect.signature(csqlite3.connect)
        self.assertEqual(signature.parameters["factory"].default,
                         csqlite3.Connection)

    def test_connect_function_execution(self):
        connection = csqlite3.connect(":memory:")
        self.assertIsInstance(connection, csqlite3.Connection)

    def test_register_adapter_and_register_converter(self):
        csqlite3.register_adapter(Point, adapt_point)
        csqlite3.register_converter("point", convert_point)
        p = Point(4.0, -3.2)
        con = csqlite3.connect(":memory:",
                               detect_types=csqlite3.PARSE_DECLTYPES)
        cur = con.cursor()
        cur.execute("create table test(p point)")

        cur.execute("insert into test(p) values (?)", (p,))
        cur.execute("select p from test")
        obtained = cur.fetchone()[0]

        cur.close()
        con.close()


if __name__ == '__main__':
    unittest.main()
