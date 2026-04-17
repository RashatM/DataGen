from app.shared.errors import UserFacingError


class InfrastructureConfigurationError(ValueError):
    """Raised when infrastructure components are wired incorrectly."""


class ConverterRegistrationError(InfrastructureConfigurationError):
    """Raised when a source converter is registered under a mismatched source type."""


class SourceValueConverterNotRegisteredError(InfrastructureConfigurationError):
    """Raised when no source value converter exists for a source data type."""


class DataGeneratorNotRegisteredError(InfrastructureConfigurationError):
    """Raised when no data generator exists for a data type."""


class UnsupportedOutputDataTypeError(InfrastructureConfigurationError):
    """Raised when DDL mapping has no representation for an output data type."""


class SchemaValidationError(UserFacingError):
    """Raised when input schema definition is invalid."""


class ObjectPayloadFormatError(ValueError):
    """Raised when object payload has an unexpected structure."""


class RunStateCorruptedError(ValueError):
    """Raised when latest run pointer exists but has invalid content."""


class ObjectNotFoundError(FileNotFoundError):
    """Raised when object does not exist in object s3."""
