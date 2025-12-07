"""Schema Management for MongoDB"""

from typing import Any, Optional, list as ListType
from ..core.connection import Connection
from ..core.exceptions import ProgrammingError


class Schema:
    """Schema management utilities for MongoDB"""

    def __init__(self, connection: Connection):
        self._conn = connection

    def create_collection(
            self,
            name: str,
            validator: dict | None = None,
            validation_level: str = "strict",
            validation_action: str = "error",
            **kwargs
    ) -> None:
        """
        Create a collection.
        
        Args:
            name: Collection name
            validator: JSON schema validator
            validation_level: Validation level (off, strict, moderate)
            validation_action: Validation action (error, warn)
            **kwargs: Additional options (capped, size, max, etc.)
        """
        try:
            db = self._conn.database
            options = kwargs.copy()
            
            if validator:
                options["validator"] = validator
                options["validationLevel"] = validation_level
                options["validationAction"] = validation_action
            
            db.create_collection(name, **options)
        except Exception as e:
            if "already exists" in str(e).lower():
                pass  # Collection already exists
            else:
                raise ProgrammingError(f"Failed to create collection: {e}")

    def drop_collection(self, name: str) -> None:
        """Drop a collection"""
        try:
            db = self._conn.database
            db.drop_collection(name)
        except Exception as e:
            raise ProgrammingError(f"Failed to drop collection: {e}")

    def create_index(
            self,
            collection: str,
            keys: ListType[tuple[str, int]] | dict,
            name: str | None = None,
            unique: bool = False,
            sparse: bool = False,
            background: bool = False,
            **kwargs
    ) -> str:
        """
        Create an index on a collection.
        
        Args:
            collection: Collection name
            keys: Index keys (list of (field, direction) tuples or dict)
            name: Index name
            unique: Create unique index
            sparse: Create sparse index
            background: Build index in background
            **kwargs: Additional index options
        """
        try:
            coll = self._conn.collection(collection)
            
            # Convert dict to list of tuples if needed
            if isinstance(keys, dict):
                keys = list(keys.items())
            
            index_options = {
                "unique": unique,
                "sparse": sparse,
                "background": background,
                **kwargs
            }
            
            if name:
                index_options["name"] = name
            
            return coll.create_index(keys, **index_options)
        except Exception as e:
            raise ProgrammingError(f"Failed to create index: {e}")

    def drop_index(self, collection: str, index_name: str) -> None:
        """Drop an index from a collection"""
        try:
            coll = self._conn.collection(collection)
            coll.drop_index(index_name)
        except Exception as e:
            raise ProgrammingError(f"Failed to drop index: {e}")

    def get_collections(self) -> ListType[str]:
        """Get list of collections in the database"""
        try:
            db = self._conn.database
            return db.list_collection_names()
        except Exception as e:
            raise ProgrammingError(f"Failed to get collections: {e}")

    def collection_exists(self, name: str) -> bool:
        """Check if a collection exists"""
        try:
            db = self._conn.database
            return name in db.list_collection_names()
        except Exception as e:
            raise ProgrammingError(f"Failed to check collection existence: {e}")

    def get_indexes(self, collection: str) -> ListType[dict]:
        """Get list of indexes for a collection"""
        try:
            coll = self._conn.collection(collection)
            return list(coll.list_indexes())
        except Exception as e:
            raise ProgrammingError(f"Failed to get indexes: {e}")

    def rename_collection(self, old_name: str, new_name: str) -> None:
        """Rename a collection"""
        try:
            db = self._conn.database
            db[old_name].rename(new_name)
        except Exception as e:
            raise ProgrammingError(f"Failed to rename collection: {e}")

