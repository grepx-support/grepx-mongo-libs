"""MongoDB Exceptions"""


class Error(Exception):
    """Base class for all MongoDB exceptions"""
    pass


class Warning(Exception):
    """Exception raised for important warnings"""
    pass


class InterfaceError(Error):
    """Exception for errors related to the database interface"""
    pass


class DatabaseError(Error):
    """Exception for errors related to the database"""
    pass


class DataError(DatabaseError):
    """Exception for errors due to problems with the processed data"""
    pass


class OperationalError(DatabaseError):
    """Exception for errors related to database operation"""
    pass


class IntegrityError(DatabaseError):
    """Exception for database integrity constraint violations"""
    pass


class InternalError(DatabaseError):
    """Exception for internal database errors"""
    pass


class ProgrammingError(DatabaseError):
    """Exception for programming errors"""
    pass


class NotSupportedError(DatabaseError):
    """Exception for unsupported operations"""
    pass

