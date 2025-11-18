"""Transaction Management for MongoDB"""

from contextlib import contextmanager
from typing import Iterator

from ..core.connection import Connection
from ..core.exceptions import DatabaseError


class Transaction:
    """Transaction context manager for MongoDB"""

    def __init__(self, connection: Connection):
        self._conn = connection
        self._session = None
        self._started = False

    def __enter__(self) -> Connection:
        """Start transaction"""
        try:
            # MongoDB transactions require a session
            self._session = self._conn.client.start_session()
            self._session.start_transaction()
            self._started = True
        except Exception as e:
            raise DatabaseError(f"Failed to start transaction: {e}")
        
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """End transaction"""
        if self._session:
            try:
                if exc_type is None:
                    self._session.commit_transaction()
                else:
                    self._session.abort_transaction()
            finally:
                self._session.end_session()
                self._session = None
        self._started = False

    @property
    def session(self):
        """Get the MongoDB session"""
        return self._session


@contextmanager
def transaction(connection: Connection) -> Iterator[Connection]:
    """Context manager for transactions"""
    with Transaction(connection) as conn:
        yield conn


class Session:
    """MongoDB session management"""

    def __init__(self, connection: Connection):
        self._conn = connection
        self._session = None

    def __enter__(self) -> 'Session':
        """Start session"""
        self._session = self._conn.client.start_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """End session"""
        if self._session:
            self._session.end_session()
            self._session = None

    @property
    def session(self):
        """Get the MongoDB session"""
        return self._session

    def start_transaction(self):
        """Start a transaction in this session"""
        if not self._session:
            raise DatabaseError("Session not started")
        self._session.start_transaction()

    def commit_transaction(self):
        """Commit the transaction"""
        if not self._session:
            raise DatabaseError("Session not started")
        self._session.commit_transaction()

    def abort_transaction(self):
        """Abort the transaction"""
        if not self._session:
            raise DatabaseError("Session not started")
        self._session.abort_transaction()


@contextmanager
def session(connection: Connection) -> Iterator[Session]:
    """Context manager for sessions"""
    with Session(connection) as sess:
        yield sess

