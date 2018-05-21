import unittest
import sqlite3

import csqlite3


class ConstantSuite(unittest.TestCase):
    def test_version(self):
        self.assertEqual(csqlite3.version, "1.0b1")

    def test_version_info(self):
        self.assertEqual(csqlite3.version_info, (1, 0, 0))

    def test_sqlite_version(self):
        self.assertEqual(csqlite3.sqlite_version, sqlite3.sqlite_version)

    def test_sqlite_version_info(self):
        self.assertEqual(csqlite3.sqlite_version_info,
                         sqlite3.sqlite_version_info)

    def test_PARSE_DECLTYPES(self):
        self.assertEqual(csqlite3.PARSE_DECLTYPES, sqlite3.PARSE_DECLTYPES)

    def test_PARSE_COLNAMES(self):
        self.assertEqual(csqlite3.PARSE_COLNAMES, sqlite3.PARSE_COLNAMES)

    def test_complete_statement(self):
        self.assertEqual(csqlite3.complete_statement,
                         sqlite3.complete_statement)

    def test_enable_callback_tracebacks(self):
        csqlite3.enable_callback_tracebacks(True)
        csqlite3.enable_callback_tracebacks(False)

if __name__ == '__main__':
    unittest.main()
