"""Query Builder and utilities for MongoDB"""

from typing import Any, Optional
from ..core.connection import Connection
from ..core.cursor import Cursor


class QueryBuilder:
    """Simple query builder for MongoDB"""

    def __init__(self, connection: Connection, collection: str):
        self._conn = connection
        self._collection = collection
        self._filter: dict = {}
        self._projection: dict | None = None
        self._sort: list[tuple] = []
        self._limit: int | None = None
        self._skip: int | None = None

    def filter(self, **kwargs) -> 'QueryBuilder':
        """Add filter conditions"""
        self._filter.update(kwargs)
        return self

    def where(self, condition: dict) -> 'QueryBuilder':
        """Add WHERE condition (alias for filter)"""
        self._filter.update(condition)
        return self

    def select(self, *fields: str) -> 'QueryBuilder':
        """Set projection fields"""
        if fields:
            self._projection = {field: 1 for field in fields}
        return self

    def exclude(self, *fields: str) -> 'QueryBuilder':
        """Exclude fields from projection"""
        if fields:
            if self._projection is None:
                self._projection = {}
            for field in fields:
                self._projection[field] = 0
        return self

    def sort(self, *fields: str, **kwargs) -> 'QueryBuilder':
        """Add sort criteria"""
        # Support both sort("field1", "field2") and sort(field1=1, field2=-1)
        if fields:
            # Default to ascending
            for field in fields:
                if field.startswith('-'):
                    self._sort.append((field[1:], -1))
                else:
                    self._sort.append((field, 1))
        if kwargs:
            for field, direction in kwargs.items():
                self._sort.append((field, 1 if direction > 0 else -1))
        return self

    def order_by(self, *fields: str) -> 'QueryBuilder':
        """Add ORDER BY (alias for sort)"""
        return self.sort(*fields)

    def limit(self, n: int) -> 'QueryBuilder':
        """Set LIMIT"""
        self._limit = n
        return self

    def skip(self, n: int) -> 'QueryBuilder':
        """Set SKIP/OFFSET"""
        self._skip = n
        return self

    def offset(self, n: int) -> 'QueryBuilder':
        """Set OFFSET (alias for skip)"""
        return self.skip(n)

    def build(self) -> dict:
        """Build MongoDB query parameters"""
        params = {
            "filter": self._filter,
        }
        if self._projection:
            params["projection"] = self._projection
        if self._sort:
            params["sort"] = self._sort
        if self._limit is not None:
            params["limit"] = self._limit
        if self._skip is not None:
            params["skip"] = self._skip
        return params

    def find_one(self) -> dict | None:
        """Execute find_one query"""
        params = self.build()
        cursor = self._conn.execute(
            self._collection,
            "find_one",
            params.get("filter", {}),
            params.get("projection")
        )
        cursor.execute()
        return cursor.fetchone()

    def find(self) -> Cursor:
        """Execute find query"""
        params = self.build()
        cursor = self._conn.execute(
            self._collection,
            "find",
            params.get("filter", {}),
            params.get("projection"),
            sort=params.get("sort"),
            limit=params.get("limit"),
            skip=params.get("skip")
        )
        cursor.execute()
        return cursor

    def fetchone(self) -> dict | None:
        """Fetch one document"""
        return self.find_one()

    def fetchall(self) -> list[dict]:
        """Fetch all documents"""
        return list(self.find())

    def fetchmany(self, size: int = 1) -> list[dict]:
        """Fetch multiple documents"""
        cursor = self.find()
        return cursor.fetchmany(size)

    def count(self) -> int:
        """Count documents matching the query"""
        params = self.build()
        cursor = self._conn.execute(
            self._collection,
            "count_documents",
            params.get("filter", {})
        )
        cursor.execute()
        return cursor.rowcount

    def update_one(self, update: dict, upsert: bool = False) -> Any:
        """Update one document"""
        params = self.build()
        cursor = self._conn.execute(
            self._collection,
            "update_one",
            params.get("filter", {}),
            update,
            upsert=upsert
        )
        cursor.execute()
        return cursor.result

    def update_many(self, update: dict, upsert: bool = False) -> Any:
        """Update many documents"""
        params = self.build()
        cursor = self._conn.execute(
            self._collection,
            "update_many",
            params.get("filter", {}),
            update,
            upsert=upsert
        )
        cursor.execute()
        return cursor.result

    def delete_one(self) -> Any:
        """Delete one document"""
        params = self.build()
        cursor = self._conn.execute(
            self._collection,
            "delete_one",
            params.get("filter", {})
        )
        cursor.execute()
        return cursor.result

    def delete_many(self) -> Any:
        """Delete many documents"""
        params = self.build()
        cursor = self._conn.execute(
            self._collection,
            "delete_many",
            params.get("filter", {})
        )
        cursor.execute()
        return cursor.result


def query(connection: Connection, collection: str) -> QueryBuilder:
    """Create a query builder"""
    return QueryBuilder(connection, collection)

