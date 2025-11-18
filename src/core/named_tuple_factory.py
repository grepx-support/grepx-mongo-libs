
class NamedTupleFactory:
    """Factory for creating named tuples from documents"""

    def __init__(self, name: str = "Document"):
        self.name = name
        self._cache: dict[tuple, type] = {}

    def __call__(self, cursor: 'Cursor', document: dict) -> tuple:
        """Create a named tuple from document"""
        if not document:
            return tuple()

        field_names = tuple(sorted(document.keys()))

        if field_names not in self._cache:
            from collections import namedtuple
            self._cache[field_names] = namedtuple(self.name, field_names)

        return self._cache[field_names](*[document[key] for key in field_names])
