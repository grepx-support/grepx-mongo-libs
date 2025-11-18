"""Connection Pool for MongoDB"""

import threading
from contextlib import contextmanager
from queue import Queue, Empty

from ..core.connection import Connection
from ..core.constants import (
    DEFAULT_MIN_CONNECTIONS, DEFAULT_MAX_CONNECTIONS, DEFAULT_CONNECTION_TIMEOUT
)
from ..core.exceptions import InterfaceError, OperationalError


class ConnectionPool:
    """Thread-safe connection pool for MongoDB"""

    def __init__(
            self,
            dsn: str | None = None,
            host: str | None = None,
            port: int | None = None,
            database: str | None = None,
            username: str | None = None,
            password: str | None = None,
            minconn: int = DEFAULT_MIN_CONNECTIONS,
            maxconn: int = DEFAULT_MAX_CONNECTIONS,
            connect_timeout: float = DEFAULT_CONNECTION_TIMEOUT,
            **kwargs
    ):
        """
        Create a connection pool.
        
        Args:
            dsn: Connection string
            host: Database host
            port: Database port
            database: Database name
            username: Username
            password: Password
            minconn: Minimum number of connections
            maxconn: Maximum number of connections
            connect_timeout: Connection timeout
            **kwargs: Additional connection parameters
        """
        self._dsn = dsn
        self._host = host
        self._port = port
        self._database = database
        self._username = username
        self._password = password
        self._minconn = minconn
        self._maxconn = maxconn
        self._connect_timeout = connect_timeout
        self._kwargs = kwargs

        self._pool: Queue[Connection] = Queue(maxsize=maxconn)
        self._created_connections = 0
        self._lock = threading.Lock()
        self._closed = False

        # Create initial connections
        for _ in range(minconn):
            self._create_connection()

    def _create_connection(self) -> Connection:
        """Create a new connection"""
        with self._lock:
            if self._closed:
                raise InterfaceError("Pool is closed")

            if self._created_connections >= self._maxconn:
                raise OperationalError("Maximum number of connections reached")

            conn = Connection(
                dsn=self._dsn,
                host=self._host,
                port=self._port,
                database=self._database,
                username=self._username,
                password=self._password,
                connect_timeout=self._connect_timeout,
                **self._kwargs
            )
            conn.connect()
            self._created_connections += 1
            return conn

    def getconn(self, timeout: float | None = None) -> Connection:
        """Get a connection from the pool"""
        if self._closed:
            raise InterfaceError("Pool is closed")

        if timeout is None:
            timeout = self._connect_timeout

        try:
            # Try to get existing connection
            conn = self._pool.get(timeout=timeout)

            # Check if connection is still alive
            try:
                # Ping the database to check connection
                conn.client.admin.command('ping')
            except Exception:
                # Connection is dead, create a new one
                conn = self._create_connection()

            return conn
        except Empty:
            # Pool is empty, try to create a new connection
            try:
                return self._create_connection()
            except OperationalError:
                # Max connections reached, wait for one to become available
                conn = self._pool.get(timeout=timeout)
                try:
                    conn.client.admin.command('ping')
                except Exception:
                    conn = self._create_connection()
                return conn

    def putconn(self, conn: Connection) -> None:
        """Return a connection to the pool"""
        if self._closed:
            conn.close()
            return

        if conn.closed:
            with self._lock:
                self._created_connections -= 1
            return

        try:
            self._pool.put_nowait(conn)
        except:
            # Pool is full, close the connection
            conn.close()
            with self._lock:
                self._created_connections -= 1

    def closeall(self) -> None:
        """Close all connections in the pool"""
        self._closed = True

        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Empty:
                break

        with self._lock:
            self._created_connections = 0

    @contextmanager
    def connection(self, timeout: float | None = None):
        """Context manager for getting a connection"""
        conn = self.getconn(timeout)
        try:
            yield conn
        finally:
            self.putconn(conn)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.closeall()


def create_pool(
        dsn: str | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        username: str | None = None,
        password: str | None = None,
        minconn: int = DEFAULT_MIN_CONNECTIONS,
        maxconn: int = DEFAULT_MAX_CONNECTIONS,
        **kwargs
) -> ConnectionPool:
    """Create a connection pool"""
    return ConnectionPool(
        dsn=dsn,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        minconn=minconn,
        maxconn=maxconn,
        **kwargs
    )
