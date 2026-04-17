from app.shared.errors import UserFacingError


class DomainError(UserFacingError):
    """Base class for expected domain-level validation and generation errors."""


class InvalidEntityError(DomainError):
    """Raised when a domain entity has invalid structural state (empty names, missing fields, bad counts, wrong mode)."""


class InvalidReferenceError(DomainError):
    """Raised when a reference points to a missing table or column."""


class InvalidConstraintsError(DomainError):
    """Raised when column constraints are malformed or incompatible."""


class InvalidDerivationError(DomainError):
    """Raised when derived column rules or source contracts are invalid."""


class UnsatisfiableConstraintsError(DomainError):
    """Raised when constraints are valid but impossible to satisfy for requested rows."""


class ValueConversionError(DomainError):
    """Raised when a concrete value cannot be converted to the requested output type."""


class DuplicateColumnSpecInTableError(DomainError):
    """Поднимается, когда спецификация таблицы содержит повторяющиеся имена колонок."""


class DuplicateTableSpecInRunError(DomainError):
    """Поднимается, когда один run пытается содержать две таблицы с одинаковым table_name."""
