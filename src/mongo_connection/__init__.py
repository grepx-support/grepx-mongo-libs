# src/mongo_connection/__init__.py (UPDATE - it's empty!)

from .core.connection import Connection, connect
from .core.exceptions import InterfaceError
from .app import MongoApp, create_app

__all__ = [
    'Connection',
    'connect',
    'InterfaceError',
    'MongoApp',
    'create_app',
]
