"""Type Adapters and Converters for MongoDB"""
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Callable
from uuid import UUID

from bson import ObjectId, Decimal128

from .exceptions import ProgrammingError


class TypeRegistry:
    """Registry for type adapters and converters"""

    _adapters: dict[type, Callable] = {}
    _converters: dict[str, Callable] = {}

    @classmethod
    def register_adapter(cls, type_: type, adapter: Callable) -> None:
        """Register an adapter for a Python type"""
        cls._adapters[type_] = adapter

    @classmethod
    def register_converter(cls, typename: str, converter: Callable) -> None:
        """Register a converter for a MongoDB/BSON type"""
        cls._converters[typename.upper()] = converter

    @classmethod
    def get_adapter(cls, type_: type) -> Callable | None:
        """Get adapter for a type"""
        # Check exact type match first
        if type_ in cls._adapters:
            return cls._adapters[type_]

        # Check for subclass matches
        for registered_type, adapter in cls._adapters.items():
            if issubclass(type_, registered_type):
                return adapter

        return None

    @classmethod
    def get_converter(cls, typename: str) -> Callable | None:
        """Get converter for a typename"""
        return cls._converters.get(typename.upper())

    @classmethod
    def adapt(cls, value: Any) -> Any:
        """Adapt a Python value to MongoDB/BSON"""
        if value is None:
            return None

        value_type = type(value)
        adapter = cls.get_adapter(value_type)

        if adapter:
            return adapter(value)

        return value

    @classmethod
    def convert(cls, typename: str, value: Any) -> Any:
        """Convert a MongoDB/BSON value to Python"""
        converter = cls.get_converter(typename)
        if converter:
            return converter(value)
        return value


# Default adapters
def adapt_datetime(val: datetime) -> datetime:
    """Adapt datetime (MongoDB handles datetime natively)"""
    return val


def adapt_date(val: date) -> datetime:
    """Adapt date to datetime (MongoDB uses datetime)"""
    return datetime.combine(val, datetime.min.time())


def adapt_decimal(val: Decimal) -> Decimal128:
    """Adapt Decimal to Decimal128"""
    return Decimal128(str(val))


def adapt_uuid(val: UUID) -> str:
    """Adapt UUID to string"""
    return str(val)


def adapt_objectid(val: ObjectId) -> ObjectId:
    """Adapt ObjectId (MongoDB handles ObjectId natively)"""
    return val


# Default converters
def convert_datetime(val: Any) -> datetime:
    """Convert MongoDB datetime to Python datetime"""
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        return datetime.fromisoformat(val.replace(' ', 'T', 1))
    raise ProgrammingError(f"Cannot convert {type(val)} to datetime")


def convert_date(val: Any) -> date:
    """Convert MongoDB datetime to Python date"""
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        return datetime.fromisoformat(val.replace(' ', 'T', 1)).date()
    raise ProgrammingError(f"Cannot convert {type(val)} to date")


def convert_decimal(val: Any) -> Decimal:
    """Convert MongoDB Decimal128 to Decimal"""
    if isinstance(val, Decimal128):
        return Decimal(str(val))
    if isinstance(val, Decimal):
        return val
    return Decimal(str(val))


def convert_uuid(val: Any) -> UUID:
    """Convert MongoDB string to Python UUID"""
    if isinstance(val, UUID):
        return val
    return UUID(str(val))


def convert_objectid(val: Any) -> ObjectId:
    """Convert to ObjectId"""
    if isinstance(val, ObjectId):
        return val
    if isinstance(val, str):
        return ObjectId(val)
    raise ProgrammingError(f"Cannot convert {type(val)} to ObjectId")


# Registration functions
def register_adapter(type_: type, adapter: Callable) -> None:
    """Register an adapter for a Python type"""
    TypeRegistry.register_adapter(type_, adapter)


def register_converter(typename: str, converter: Callable) -> None:
    """Register a converter for a MongoDB/BSON type"""
    TypeRegistry.register_converter(typename, converter)


# Register default adapters
TypeRegistry.register_adapter(datetime, adapt_datetime)
TypeRegistry.register_adapter(date, adapt_date)
TypeRegistry.register_adapter(Decimal, adapt_decimal)
TypeRegistry.register_adapter(UUID, adapt_uuid)
TypeRegistry.register_adapter(ObjectId, adapt_objectid)

# Register default converters
TypeRegistry.register_converter("DATETIME", convert_datetime)
TypeRegistry.register_converter("DATE", convert_date)
TypeRegistry.register_converter("DECIMAL128", convert_decimal)
TypeRegistry.register_converter("DECIMAL", convert_decimal)
TypeRegistry.register_converter("UUID", convert_uuid)
TypeRegistry.register_converter("OBJECTID", convert_objectid)
TypeRegistry.register_converter("OID", convert_objectid)
