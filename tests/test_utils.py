import unittest

from csqlite3 import utils


class Suite(unittest.TestCase):
    def test_require_function(self):
        sql1 = utils.require("sqlite3")
        sql2 = utils.require("sqlite3")
        self.assertIsNot(sql1, sql2)
