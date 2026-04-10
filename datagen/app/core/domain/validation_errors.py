class InvalidForeignKeyError(ValueError):
    """Raised when a foreign key points to a missing table or column."""


class InvalidConstraintsError(ValueError):
    """Raised when column constraints are malformed or incompatible."""


class InvalidDerivationError(ValueError):
    """Raised when derived column rules or source contracts are invalid."""


class UnsatisfiableConstraintsError(ValueError):
    """Raised when constraints are valid but impossible to satisfy for requested rows."""


class ValueConversionError(ValueError):
    """Raised when a concrete value cannot be converted to the requested output type."""
