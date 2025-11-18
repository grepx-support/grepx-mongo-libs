"""ORM-like Model Base Class for MongoDB"""

from typing import Any, Optional, ClassVar
from bson import ObjectId
from ..core.connection import Connection
from ..core.exceptions import ProgrammingError


class Model:
    """Base model class for simple ORM-like functionality with MongoDB"""

    _collection: ClassVar[str | None] = None
    _connection: ClassVar[Connection | None] = None
    _id: Optional[ObjectId] = None

    @classmethod
    def set_connection(cls, connection: Connection) -> None:
        """Set the database connection for this model"""
        cls._connection = connection

    @classmethod
    def _get_collection_name(cls) -> str:
        """Get collection name (defaults to lowercase class name)"""
        if cls._collection:
            return cls._collection
        return cls.__name__.lower()

    def save(self, connection: Connection | None = None) -> None:
        """Save the model instance"""
        conn = connection or self._connection
        if not conn:
            raise ProgrammingError("No connection available")
        
        collection_name = self._get_collection_name()
        collection = conn.collection(collection_name)
        
        # Get fields and values (exclude _id and private attributes)
        data = {}
        for key, value in self.__dict__.items():
            if not key.startswith('_') or key == '_id':
                data[key] = value
        
        if self._id:
            # Update existing document
            result = collection.update_one(
                {"_id": self._id},
                {"$set": data}
            )
            if result.matched_count == 0:
                raise ProgrammingError("Document not found for update")
        else:
            # Insert new document
            result = collection.insert_one(data)
            self._id = result.inserted_id

    def delete(self, connection: Connection | None = None) -> None:
        """Delete the model instance"""
        if not self._id:
            raise ProgrammingError("Cannot delete unsaved document")
        
        conn = connection or self._connection
        if not conn:
            raise ProgrammingError("No connection available")
        
        collection_name = self._get_collection_name()
        collection = conn.collection(collection_name)
        collection.delete_one({"_id": self._id})
        self._id = None

    @classmethod
    def find(cls, id: Any, connection: Connection | None = None) -> Optional['Model']:
        """Find a record by ID"""
        conn = connection or cls._connection
        if not conn:
            raise ProgrammingError("No connection available")
        
        collection_name = cls._get_collection_name()
        collection = conn.collection(collection_name)
        
        # Convert string to ObjectId if needed
        if isinstance(id, str):
            try:
                id = ObjectId(id)
            except:
                pass
        
        doc = collection.find_one({"_id": id})
        
        if doc:
            return cls._from_document(doc)
        return None

    @classmethod
    def find_one(cls, filter: dict, connection: Connection | None = None) -> Optional['Model']:
        """Find one document matching filter"""
        conn = connection or cls._connection
        if not conn:
            raise ProgrammingError("No connection available")
        
        collection_name = cls._get_collection_name()
        collection = conn.collection(collection_name)
        doc = collection.find_one(filter)
        
        if doc:
            return cls._from_document(doc)
        return None

    @classmethod
    def find_many(cls, filter: dict = None, connection: Connection | None = None) -> list['Model']:
        """Find many documents matching filter"""
        conn = connection or cls._connection
        if not conn:
            raise ProgrammingError("No connection available")
        
        collection_name = cls._get_collection_name()
        collection = conn.collection(collection_name)
        
        if filter is None:
            filter = {}
        
        cursor = collection.find(filter)
        return [cls._from_document(doc) for doc in cursor]

    @classmethod
    def all(cls, connection: Connection | None = None) -> list['Model']:
        """Get all records"""
        return cls.find_many({}, connection)

    @classmethod
    def _from_document(cls, doc: dict) -> 'Model':
        """Create model instance from database document"""
        instance = cls.__new__(cls)
        
        for key, value in doc.items():
            setattr(instance, key, value)
        
        return instance

    def to_dict(self) -> dict:
        """Convert model to dictionary"""
        data = {}
        for key, value in self.__dict__.items():
            if not key.startswith('_') or key == '_id':
                data[key] = value
        return data

    def __repr__(self) -> str:
        """String representation"""
        return f"<{self.__class__.__name__} _id={self._id}>"

