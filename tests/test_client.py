import hashlib
import inspect
import itertools
import pathlib
import sqlite3
import unittest

import csqlite3


# Helpers
# =======


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


class IterChars:
    def __init__(self):
        self.count = ord('a')

    def __iter__(self):
        return self

    def __next__(self):
        if self.count > ord('z'):
            raise StopIteration
        self.count += 1
        return (chr(self.count - 1),) # this is a 1-tuple


def md5sum(t):
    return hashlib.md5(t).hexdigest()


class MySum:
    def __init__(self):
        self.count = 0

    def step(self, value):
        self.count += value

    def finalize(self):
        return self.count


def collate_reverse(string1, string2):
    if string1 == string2:
        return 0
    elif string1 < string2:
        return 1
    else:
        return -1


def authorizer(action, arg1, arg2, db_name, trigger_name):
    if action == csqlite3.SQLITE_DELETE and arg1 == "users":
        return csqlite3.SQLITE_DENY  # 1
    elif action == csqlite3.SQLITE_READ \
    and arg1 == "users" and arg2 == "password":
        return csqlite3.SQLITE_IGNORE  # 2
    return csqlite3.SQLITE_OK  # 0


pcount = 0
counter = itertools.count()


def progress_function():
    global pcount
    pcount += 1
    return 0


traced_statement = []


def trace_function(statement):
    traced_statement.append(statement)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


# Test cases
# ==========

class ModuleSuite(unittest.TestCase):
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
        cur.execute("create table test_2(p point)")

        cur.execute("insert into test_2(p) values (?)", (p,))
        cur.execute("select p from test_2")
        obtained = cur.fetchone()[0]
        self.assertIsInstance(obtained, Point)

        cur.close()
        con.close()


class ConnectionSuite(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.connection = csqlite3.Connection(":memory:")
        self.cursor = self.connection.cursor()

    @classmethod
    def tearDownClass(self):
        self.connection.close()

    def test_connection_instance(self):
        self.assertIsInstance(self.connection, csqlite3.Connection)

    def test_isolation_level(self):
        self.assertEqual(self.connection.isolation_level, "")

    def test_in_transaction(self):
        self.assertTrue(hasattr(self.connection, "in_transaction"))

    def test_commit(self):
        self.connection.commit()

    def test_rollback(self):
        self.connection.rollback()

    def test_execute(self):
        self.connection.execute("create table people (name, age)")
        cursor = self.connection.execute("insert into people values (?, ?)",
                                        ("Mirtha", 100))
        self.assertIsInstance(cursor, csqlite3.Cursor)
        self.cursor.execute(
            "select * from people where name=:who and age=:age",
            {"who": "Mirtha", "age": 100})
        obtained = self.cursor.fetchone()
        self.assertEqual(obtained, ("Mirtha", 100))

    def test_executemany(self):
        self.connection.execute("create table characters(c)")
        cursor = self.connection \
            .executemany("insert into characters(c) values (?)", IterChars())
        self.assertIsInstance(cursor, csqlite3.Cursor)
        self.cursor.execute("select c from characters")
        self.assertEqual(self.cursor.fetchall(), list(IterChars()))

    def test_executescript(self):
        cursor = self.connection.executescript("""
            create table person(
                firstname,
                lastname,
                age
            );
            create table book(
                title,
                author,
                published
            );
            insert into book(title, author, published)
            values (
                'Dirk Gently''s Holistic Detective Agency',
                'Douglas Adams',
                1987
            );
            """)
        self.assertIsInstance(cursor, csqlite3.Cursor)
        self.cursor.execute("select * from book")
        obtained = self.cursor.fetchone()
        expected = ("Dirk Gently's Holistic Detective Agency",
                    'Douglas Adams', 1987)
        self.assertEqual(obtained, expected)

    def test_create_function(self):
        self.connection.create_function("md5", 1, md5sum)
        self.cursor.execute("select md5(?)", (b"foo",))
        self.assertEqual(self.cursor.fetchone()[0],
                         "acbd18db4cc2f85cedef654fccc4a4d8")

    def test_create_aggregate(self):
        self.connection.create_aggregate("mysum", 1, MySum)
        self.cursor.execute("create table test_0(i)")
        self.cursor.execute("insert into test_0(i) values (1)")
        self.cursor.execute("insert into test_0(i) values (2)")
        self.cursor.execute("select mysum(i) from test_0")
        self.assertEqual(self.cursor.fetchone()[0], 3)

    def test_create_collation(self):
        self.connection.create_collation("reverse", collate_reverse)
        self.cursor.execute("create table test_1(x)")
        self.cursor.executemany("insert into test_1(x) values (?)",
                                [("a",), ("b",)])
        self.cursor.execute("select x from test_1 order by x collate reverse")
        self.assertEqual(list(self.cursor), [("b",), ("a",)])

    def test_remove_collation(self):
        self.connection.create_collation("reverse", collate_reverse)
        self.connection.create_collation("reverse", None)

    def test_interrupt(self):
        self.connection.interrupt()

    def test_set_authorizer(self):
        self.connection.execute(
            "CREATE TABLE users (username TEXT PRIMARY KEY, password TEXT)")
        self.connection.execute("INSERT INTO users (username, password) "
                                "VALUES (?, ?), (?, ?)",
                                ("huey", "meow", "mickey", "woof"))
        self.cursor.execute('SELECT * FROM users;')
        obtained = self.cursor.fetchall()
        expected = [('huey', "meow"), ('mickey', "woof")]
        self.assertEqual(obtained, expected)
        self.connection.set_authorizer(authorizer)
        with self.assertRaisesRegex(sqlite3.DatabaseError, "not authorized"):
            self.connection.execute("DELETE FROM users WHERE username = ?",
                                    ("huey",))

    def test_set_progress_handler(self):
        self.connection.set_progress_handler(progress_function, 1)
        self.connection.execute("CREATE TABLE progress (step)")
        self.assertEqual(pcount, 12)

    def test_set_trace_callback(self):
        self.connection.set_trace_callback(trace_function)
        self.connection.execute("CREATE TABLE trace(a, b)")
        self.assertTrue(traced_statement)

    def test_enable_load_extension(self):
        self.connection.enable_load_extension(True)
        self.connection.enable_load_extension(False)

    def test_load_extension(self):
        self.connection.load_extension

    def test_row_factory(self):
        self.connection.row_factory = dict_factory
        cursor = self.connection.cursor()
        cursor.execute("select 1 as a")
        self.assertEqual(cursor.fetchone()["a"], 1)
        cursor.execute("select 2 as b")
        result = cursor.fetchall()
        self.assertEqual(result[0]["b"], 2)
        self.connection.row_factory = None

    def test_text_factory(self):
        self.connection.text_factory = bytes
        self.cursor.execute("SELECT ?", ["text1"])
        self.assertIsInstance(self.cursor.fetchone()[0], bytes)
        self.cursor.execute("SELECT ?", ["text2"])
        self.assertIsInstance(self.cursor.fetchall()[0][0], bytes)
        self.connection.text_factory = None
        self.cursor.text_factory = None

    def test_total_changes(self):
        self.assertGreaterEqual(self.connection.total_changes, 10)

    def test_iterdump(self):
        path = str(pathlib.Path("tests")/"iterdump_example.db")
        con = csqlite3.connect(path)
        obtained = list(con.iterdump())
        con.close()
        expected = ['BEGIN TRANSACTION;',
                    'CREATE TABLE data(item);',
                    'INSERT INTO "data" VALUES(1);',
                    'INSERT INTO "data" VALUES(2);',
                    'INSERT INTO "data" VALUES(3);',
                    'INSERT INTO "data" VALUES(4);',
                    'COMMIT;']
        self.assertEqual(obtained, expected)


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
        self.cursor.execute("CREATE TABLE cursor_1 (p INT)")
        self.cursor.execute("INSERT INTO cursor_1 (p) VALUES (1)")
        self.cursor.execute("SELECT p FROM cursor_1 WHERE p=1")
        obtained = self.cursor.fetchone()[0]
        self.assertEqual(obtained, 1)

    def test_fetchall(self):
        self.cursor.execute("CREATE TABLE cursor_2 (p INT)")
        self.cursor.execute("INSERT INTO cursor_2 (p) VALUES (2)")
        self.cursor.execute("SELECT p FROM cursor_2 WHERE p=2")
        obtained = self.cursor.fetchall()
        self.assertEqual(obtained, [(2,)])

    def test_fetchmany(self):
        self.cursor.execute("CREATE TABLE cursor_3 (p INT)") \
                   .executemany("INSERT INTO cursor_3 (p) VALUES (?)",
                                [[1], [2], [3]]) \
                   .execute("SELECT p FROM cursor_3")
        obtained = self.cursor.fetchmany(3)
        expected = [(1,), (2,), (3,)]
        self.assertEqual(obtained, expected)

    def test_rowcount(self):
        self.cursor.execute("CREATE TABLE cursor_4 (p INT)") \
                   .executemany("INSERT INTO cursor_4 (p) VALUES (?)",
                                [[1], [2], [3]])
        self.assertEqual(self.cursor.rowcount, 3)

    def test_lastrowid(self):
        self.cursor.execute("CREATE TABLE cursor_5(p INT)") \
                   .execute("INSERT INTO cursor_5 (p) VALUES (1)")
        self.assertEqual(self.cursor.lastrowid, 1)

    def test_arraysize(self):
        self.assertEqual(self.cursor.arraysize, 1)
        self.cursor.arraysize = 2
        self.assertEqual(self.cursor.arraysize, 2)


if __name__ == '__main__':
    unittest.main()
