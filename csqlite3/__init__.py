import sqlite3

# from .client import connect


__all__ = ["version", "version_info", "sqlite_version", "sqlite_version_info",
           "PARSE_DECLTYPES", "PARSE_COLNAMES", "connect"]


# Constants

version = "1.0b1"
version_info = (1, 0, 0)
sqlite_version = sqlite3.sqlite_version
sqlite_version_info = sqlite3.sqlite_version_info
PARSE_DECLTYPES = sqlite3.PARSE_DECLTYPES
PARSE_COLNAMES = sqlite3.PARSE_COLNAMES
