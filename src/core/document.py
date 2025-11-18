"""Document Factories and Document Objects"""

from typing import Any, Mapping


class Document(Mapping):
    """A document from a MongoDB query result"""

    def __init__(self, cursor: 'Cursor', data: dict):
        self._cursor = cursor
        self._data = data

    def __getitem__(self, key: str) -> Any:
        """Get item by key"""
        return self._data[key]

    def __len__(self) -> int:
        """Return number of fields"""
        return len(self._data)

    def __iter__(self):
        """Iterate over keys"""
        return iter(self._data)

    def __eq__(self, other) -> bool:
        """Compare documents"""
        if isinstance(other, Document):
            return self._data == other._data
        if isinstance(other, dict):
            return self._data == other
        return False

    def __hash__(self) -> int:
        """Hash the document"""
        return hash(tuple(sorted(self._data.items())))

    def __repr__(self) -> str:
        """String representation"""
        return f"Document({self._data})"

    def keys(self):
        """Return field names"""
        return self._data.keys()

    def values(self):
        """Return field values"""
        return self._data.values()

    def items(self):
        """Return (field_name, value) pairs"""
        return self._data.items()

    def get(self, key: str, default: Any = None) -> Any:
        """Get value by key with default"""
        return self._data.get(key, default)

    def to_dict(self) -> dict:
        """Convert document to dictionary"""
        return self._data.copy()

