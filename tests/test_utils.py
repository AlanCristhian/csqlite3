import pickle
import socket
import struct
import threading
import time
import unittest

from csqlite3 import utils


pcount = 0


def progress_function():
    global pcount
    pcount += 1
    return 0


def alert_progress(address):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect(address)
        sock.send(b"0")


traced_statement = []


def trace_function(statement):
    traced_statement.append(statement)


def alert_trace(address):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect(address)
        data = [{}, 1, 2.0, "a", b"b"]
        serialized = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)
        header = struct.pack("!i", len(serialized))
        sock.sendall(header + serialized)


class Suite(unittest.TestCase):
    def test_require(self):
        sql1 = utils.require("sqlite3")
        sql2 = utils.require("sqlite3")
        self.assertIsNot(sql1, sql2)

    def test_new_progress_server(self):
        server = utils.new_progress_server(progress_function)
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()
        alert_progress(server.server_address)
        alert_progress(server.server_address)
        self.assertGreaterEqual(pcount, 1)
        server.shutdown()

    def test_new_trace_server(self):
        server = utils.new_trace_server(trace_function)
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()
        alert_trace(server.server_address)
        alert_trace(server.server_address)
        self.assertTrue(traced_statement)

    def test_convert_str_to(self):
        to_bytes = lambda string: bytes(string, "ascii")
        obtained = utils.convert_str_to(to_bytes, [("a", 1)])
        expected = [(b"a", 1)]
        self.assertEqual(obtained, expected)
        obtained = utils.convert_str_to(to_bytes, ("b", 2))
        expected = (b"b", 2)
        self.assertEqual(obtained, expected)


if __name__ == '__main__':
    unittest.main()
