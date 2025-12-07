# libs/mongo_connection/src/app.py

from omegaconf import DictConfig
from .core.connection import connect
from .core.exceptions import InterfaceError


class MongoApp:
    """MongoDB application wrapper"""
    
    def __init__(self, config: DictConfig):
        self.config = config
        self._connection = None
    
    @property
    def connection(self):
        """Lazy connection"""
        if self._connection is None:
            self._connection = self._create_connection()
        return self._connection
    
    @property
    def db(self):
        """Get database"""
        return self.connection.database
    
    @property
    def client(self):
        """Get MongoDB client"""
        return self.connection.client
    
    def collection(self, name: str = None):
        """Get collection"""
        if name is None:
            name = self.config.mongodb.get('collection', 'default')
        return self.connection.collection(name)
    
    def _create_connection(self):
        """Create connection from config"""
        conn_type = self.config.mongodb.get('connection_type', 'local')
        
        if conn_type == 'atlas':
            return connect(
                dsn=self.config.mongodb.atlas.connection_string,
                database=self.config.mongodb.atlas.get('database', self.config.mongodb.database)
            )
        else:
            local = self.config.mongodb.local
            return connect(
                host=local.get('host', 'localhost'),
                port=local.get('port', 27017),
                database=local.get('database', 'test'),
                username=local.get('username'),
                password=local.get('password'),
                auth_source=local.get('auth_source', 'admin')
            )
    
    def close(self):
        """Close connection"""
        if self._connection:
            self._connection.close()
            self._connection = None


def create_app(config: DictConfig) -> MongoApp:
    """Create MongoDB app from config"""
    return MongoApp(config)