"""Connection Objects for MongoDB"""

import threading

from .constants import (
    DEFAULT_READ_PREFERENCE, DEFAULT_WRITE_CONCERN, DEFAULT_READ_CONCERN,
    DEFAULT_PORT, DEFAULT_HOST, DEFAULT_DATABASE
)
from .cursor import Cursor
from .document_factory import DocumentFactory
from .exceptions import InterfaceError


class Connection:
    """MongoDB database connection"""

    def __init__(
            self,
            dsn: str | None = None,
            host: str | None = None,
            port: int | None = None,
            database: str | None = None,
            username: str | None = None,
            password: str | None = None,
            auth_source: str | None = None,
            auth_mechanism: str | None = None,
            connect_timeout: float = 20.0,
            server_selection_timeout: float = 30.0,
            socket_timeout: float | None = None,
            read_preference: str = DEFAULT_READ_PREFERENCE,
            write_concern: int | str = DEFAULT_WRITE_CONCERN,
            read_concern: str = DEFAULT_READ_CONCERN,
            document_factory: type | None = None,
    ):
        """
        Create a MongoDB connection.
        
        Args:
            dsn: Connection string (e.g., "mongodb://user:pass@host:port/db")
            host: Database host
            port: Database port (default: 27017)
            database: Database name
            username: Username
            password: Password
            auth_source: Authentication database
            auth_mechanism: Authentication mechanism
            connect_timeout: Connection timeout in seconds
            server_selection_timeout: Server selection timeout in seconds
            socket_timeout: Socket timeout in seconds
            read_preference: Read preference (primary, secondary, etc.)
            write_concern: Write concern level
            read_concern: Read concern level
            document_factory: Custom document factory
        """
        self._dsn = dsn
        self._host = host
        self._port = port
        self._database = database
        self._username = username
        self._password = password
        self._auth_source = auth_source
        self._auth_mechanism = auth_mechanism
        self._connect_timeout = connect_timeout
        self._server_selection_timeout = server_selection_timeout
        self._socket_timeout = socket_timeout
        self._read_preference = read_preference
        self._write_concern = write_concern
        self._read_concern = read_concern
        self._thread_ident = threading.get_ident()
        self._document_factory: DocumentFactory | None = document_factory
        self._client = None
        self._db = None
        self._closed = False

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def connect(self):
        """Establish connection to MongoDB"""
        if self._client is not None:
            return

        try:
            from pymongo import MongoClient
            from pymongo.read_preferences import ReadPreference
            from pymongo.write_concern import WriteConcern
            from pymongo.read_concern import ReadConcern

            # Build connection parameters
            if self._dsn:
                client_params = {"host": self._dsn}
            else:
                client_params = {
                    "host": self._host or DEFAULT_HOST,
                    "port": self._port or DEFAULT_PORT,
                }
                if self._username:
                    client_params["username"] = self._username
                if self._password:
                    client_params["password"] = self._password
                if self._auth_source:
                    client_params["authSource"] = self._auth_source
                if self._auth_mechanism:
                    client_params["authMechanism"] = self._auth_mechanism

            client_params.update({
                "connectTimeoutMS": int(self._connect_timeout * 1000),
                "serverSelectionTimeoutMS": int(self._server_selection_timeout * 1000),
            })
            if self._socket_timeout:
                client_params["socketTimeoutMS"] = int(self._socket_timeout * 1000)

            # Create client
            self._client = MongoClient(**client_params)

            # Get database
            db_name = self._database or DEFAULT_DATABASE
            self._db = self._client[db_name]

            # Set read preference, write concern, read concern
            if self._read_preference:
                read_pref_map = {
                    "primary": ReadPreference.PRIMARY,
                    "primaryPreferred": ReadPreference.PRIMARY_PREFERRED,
                    "secondary": ReadPreference.SECONDARY,
                    "secondaryPreferred": ReadPreference.SECONDARY_PREFERRED,
                    "nearest": ReadPreference.NEAREST,
                }
                read_pref = read_pref_map.get(self._read_preference, ReadPreference.PRIMARY)
                self._db = self._db.with_options(read_preference=read_pref)

            if self._write_concern:
                if isinstance(self._write_concern, str):
                    wc = WriteConcern(w=self._write_concern)
                else:
                    wc = WriteConcern(w=self._write_concern)
                self._db = self._db.with_options(write_concern=wc)

            if self._read_concern:
                rc = ReadConcern(level=self._read_concern)
                self._db = self._db.with_options(read_concern=rc)

        except ImportError:
            raise InterfaceError("pymongo is required. Install it with: pip install pymongo")
        except Exception as e:
            raise InterfaceError(f"Failed to connect to MongoDB: {e}")

    def close(self):
        """Close the connection"""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
        self._closed = True

    @property
    def closed(self) -> bool:
        """Check if connection is closed"""
        return self._closed

    def execute(self, collection: str, operation: str, *args, **kwargs) -> Cursor:
        """Execute a MongoDB operation"""
        if self._closed or self._db is None:
            raise InterfaceError("Connection is closed")

        return Cursor(self, collection, operation, *args, **kwargs)

    def collection(self, name: str):
        """Get a collection object"""
        if self._closed or self._db is None:
            raise InterfaceError("Connection is closed")
        return self._db[name]

    @property
    def database(self):
        """Get the database object"""
        if self._closed or self._db is None:
            raise InterfaceError("Connection is closed")
        return self._db

    @property
    def client(self):
        """Get the client object"""
        if self._closed or self._client is None:
            raise InterfaceError("Connection is closed")
        return self._client


def connect(
        dsn: str | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        username: str | None = None,
        password: str | None = None,
        **kwargs
) -> Connection:
    """Create a MongoDB connection"""
    conn = Connection(
        dsn=dsn,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        **kwargs
    )
    conn.connect()
    return conn
