from typing import Any, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .cursor import Cursor
    from .document import Document
else:
    Document = None  # Will be imported when needed


class DocumentFactory(Protocol):
    """Protocol for document factory functions"""

    def __call__(self, cursor: 'Cursor', document: dict) -> Any:
        """Convert a database document to Python object"""
        ...


def document_factory(cursor: 'Cursor', document: dict) -> 'Document':
    """Default document factory that returns Document objects"""
    from .document import Document
    return Document(cursor, document)


def dict_factory(cursor: 'Cursor', document: dict) -> dict:
    """Document factory that returns dictionaries (default MongoDB behavior)"""
    return document
