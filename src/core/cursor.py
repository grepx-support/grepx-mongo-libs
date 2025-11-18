"""Cursor Objects for MongoDB"""

from typing import Any, Iterator, Optional
from collections.abc import Sequence

from .exceptions import InterfaceError, ProgrammingError
from .document import DocumentFactory, document_factory, dict_factory


class Cursor:
    """MongoDB cursor for query results"""

    def __init__(
            self,
            connection: 'Connection',
            collection: str,
            operation: str,
            *args,
            **kwargs
    ):
        """
        Create a cursor for MongoDB operations.
        
        Args:
            connection: Database connection
            collection: Collection name
            operation: Operation type (find, find_one, insert_one, etc.)
            *args: Operation arguments
            **kwargs: Operation keyword arguments
        """
        self._connection = connection
        self._collection_name = collection
        self._operation = operation
        self._args = args
        self._kwargs = kwargs
        self._document_factory: DocumentFactory = (
            connection._document_factory or dict_factory
        )
        self._result = None
        self._executed = False
        self._description = None

    def execute(self) -> 'Cursor':
        """Execute the operation"""
        if self._executed:
            return self

        if self._connection.closed:
            raise InterfaceError("Connection is closed")

        try:
            collection = self._connection.collection(self._collection_name)

            if self._operation == "find":
                self._result = collection.find(*self._args, **self._kwargs)
                self._description = None  # MongoDB doesn't have fixed schema
            elif self._operation == "find_one":
                self._result = collection.find_one(*self._args, **self._kwargs)
            elif self._operation == "insert_one":
                self._result = collection.insert_one(*self._args, **self._kwargs)
            elif self._operation == "insert_many":
                self._result = collection.insert_many(*self._args, **self._kwargs)
            elif self._operation == "update_one":
                self._result = collection.update_one(*self._args, **self._kwargs)
            elif self._operation == "update_many":
                self._result = collection.update_many(*self._args, **self._kwargs)
            elif self._operation == "delete_one":
                self._result = collection.delete_one(*self._args, **self._kwargs)
            elif self._operation == "delete_many":
                self._result = collection.delete_many(*self._args, **self._kwargs)
            elif self._operation == "replace_one":
                self._result = collection.replace_one(*self._args, **self._kwargs)
            elif self._operation == "aggregate":
                self._result = collection.aggregate(*self._args, **self._kwargs)
            elif self._operation == "count_documents":
                self._result = collection.count_documents(*self._args, **self._kwargs)
            else:
                raise ProgrammingError(f"Unknown operation: {self._operation}")

            self._executed = True
        except Exception as e:
            raise ProgrammingError(f"Operation failed: {e}")

        return self

    def fetchone(self) -> dict | None:
        """Fetch one document"""
        if not self._executed:
            self.execute()

        if self._operation == "find_one":
            doc = self._result
            if doc is None:
                return None
            return self._document_factory(self, doc)

        if self._operation == "find":
            if self._result is None:
                return None
            try:
                doc = next(self._result)
                return self._document_factory(self, doc)
            except StopIteration:
                return None

        # For other operations, return the result object
        return self._result

    def fetchmany(self, size: int = 1) -> list[dict]:
        """Fetch multiple documents"""
        if not self._executed:
            self.execute()

        if self._operation == "find":
            if self._result is None:
                return []
            docs = []
            try:
                for _ in range(size):
                    doc = next(self._result)
                    docs.append(self._document_factory(self, doc))
            except StopIteration:
                pass
            return docs

        if self._operation == "find_one":
            doc = self.fetchone()
            return [doc] if doc is not None else []

        return []

    def fetchall(self) -> list[dict]:
        """Fetch all documents"""
        if not self._executed:
            self.execute()

        if self._operation == "find":
            if self._result is None:
                return []
            return [
                self._document_factory(self, doc)
                for doc in self._result
            ]

        if self._operation == "find_one":
            doc = self.fetchone()
            return [doc] if doc is not None else []

        return []

    def __iter__(self) -> Iterator[dict]:
        """Iterate over results"""
        if not self._executed:
            self.execute()

        if self._operation == "find":
            if self._result is None:
                return
            for doc in self._result:
                yield self._document_factory(self, doc)
        elif self._operation == "find_one":
            doc = self.fetchone()
            if doc is not None:
                yield doc

    def __next__(self) -> dict:
        """Get next document"""
        doc = self.fetchone()
        if doc is None:
            raise StopIteration
        return doc

    @property
    def description(self) -> Optional[Sequence]:
        """Return cursor description (MongoDB doesn't have fixed schema)"""
        return self._description

    @property
    def rowcount(self) -> int:
        """Return number of affected rows (for write operations)"""
        if not self._executed:
            return -1

        if self._operation in ("insert_one", "update_one", "delete_one", "replace_one"):
            return 1 if self._result.acknowledged else 0
        elif self._operation in ("insert_many", "update_many", "delete_many"):
            return self._result.inserted_count if hasattr(self._result, 'inserted_count') else (
                self._result.modified_count if hasattr(self._result, 'modified_count') else (
                    self._result.deleted_count if hasattr(self._result, 'deleted_count') else 0
                )
            )
        elif self._operation == "count_documents":
            return self._result

        return -1

    @property
    def result(self) -> Any:
        """Get the raw result object"""
        return self._result

