from typing import Any, Protocol

from core.document import Document


class DocumentFactory(Protocol):
    """Protocol for document factory functions"""

    def __call__(self, cursor: 'Cursor', document: dict) -> Any:
        """Convert a database document to Python object"""
        ...


def document_factory(cursor: 'Cursor', document: dict) -> Document:
    """Default document factory that returns Document objects"""
    return Document(cursor, document)


def dict_factory(document: dict) -> dict:
    """Document factory that returns dictionaries (default MongoDB behavior)"""
    return document
