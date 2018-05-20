import multiprocessing
import pathlib
import platform
import signal
import socket
import subprocess
import sys
import time
import unittest

from csqlite3 import utils
from tests import test_utils, test_server, test_client, test_csqlite3


MODULES = [
    "csqlite3/utils.py",
    "csqlite3/server.py",
    "csqlite3/client.py",
]
TIMEOUT = 5


def print_dot():
    """Write a single dot"""
    sys.stdout.write(".")
    sys.stdout.flush()


def run_linter_once(linter, args):
    """Run a linter program from the command line."""
    process = subprocess.Popen(
        args,
        start_new_session=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    output, _ = process.communicate()
    if output:
        print()
        print(linter)
        print("="*len(linter))
        print(output.decode())
    return output


def run_linter_many(linter, *args):
    """Run the linter one time for module in MODULES constant."""
    for module in MODULES:
        arguments = [sys.executable, "-m", linter, module, *args]
        have_output = run_linter_once(linter, arguments)
        if have_output:
            return True
    return False


def catch_parallel_linter_errors(linter, args):
    """Run a linter and show their errors."""
    print_dot()
    arguments = [sys.executable, "-m", linter, *args]
    have_output = run_linter_once(linter, arguments)
    if have_output:
        return True
    return False


def catch_sequential_linter_errors(linter, args):
    """Run a linter and show their errors."""
    print_dot()
    have_output = run_linter_many(linter, *args)
    if have_output:
        return True
    return False


class ServerMethods:
    """This class group all methods related with the server handling."""
    def __init__(self):
        # This two properties will be defined in the child of this class.
        self.process = None

    def start_server(self):
        """Run the aiohttp server."""
        args = [sys.executable, "-m", "csqlite3.server"]
        self.process = subprocess.Popen(args, start_new_session=True)
        start = time.perf_counter()
        with socket.socket() as test_socket:
            while time.perf_counter() - start < TIMEOUT:
                try:
                    test_socket.connect((utils.HOST, utils.PORT))
                except (ConnectionAbortedError, ConnectionRefusedError):
                    continue
                break
            else:
                self.close_server()
                raise TimeoutError("aiohttp server never started.")

    def close_server(self):
        """Shut down aiohttp server."""
        if platform.system() == "Windows":
            self.process.send_signal(signal.CTRL_C_EVENT)
        else:
            self.process.send_signal(signal.SIGINT)
        start = time.perf_counter()
        while time.perf_counter() - start < TIMEOUT:
            try:
                with socket.socket() as test_socket:
                    test_socket.connect((utils.HOST, utils.PORT))
                    continue
            except (ConnectionAbortedError, ConnectionRefusedError,
                    ConnectionResetError, KeyboardInterrupt):
                break
        else:
            raise TimeoutError("aiohttp server never closed.")


class Runner(ServerMethods):
    """Set up and run test, servers, and linters."""
    def __init__(self):
        self.catch_linter_errors()
        self.start_server()
        modules = [
            test_utils,
            test_client,
            test_csqlite3,
        ]
        self.load_and_run_tests(modules)
        # On windows I need to close te server to read the log file.
        self.close_server()
        self.load_and_run_tests([test_server])

    def catch_linter_errors(self):
        """Returns True if some linter found an error. False in otherwise.
        """
        pylint_args = ["--rcfile=pylint.ini", "-d I",
                       "-j %d" % multiprocessing.cpu_count(), *MODULES]
        pycodestyle_args = ["--exclude", "logs,runtests.py,"]
        start = time.perf_counter()

        # or catch_sequential_linter_errors("mccabe", ["--min", "6"]) \
        if catch_parallel_linter_errors("pyflakes", ["csqlite3"]) \
        or catch_parallel_linter_errors("pylint", pylint_args) \
        or catch_parallel_linter_errors("bandit", ["-r", "csqlite3"]) \
        or catch_sequential_linter_errors("pycodestyle", ["csqlite3/"]) \
        or catch_sequential_linter_errors("pydocstyle", pycodestyle_args):
            return

        end = time.perf_counter() - start
        print("\n%s\nRan 5 linters in %.3fs\n\nOK" % ("-"*70, end))

    def load_and_run_tests(self, modules):
        loader = unittest.TestLoader()
        runner = unittest.TextTestRunner()
        suite = unittest.TestSuite()
        for module in modules:
            suite.addTest(loader.loadTestsFromModule(module))
        result = runner.run(suite)


if __name__ == "__main__":
    Runner()
