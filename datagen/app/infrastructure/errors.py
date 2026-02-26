class InfrastructureConfigurationError(ValueError):
    """Raised when infrastructure components are wired incorrectly."""


class ConverterRegistrationError(InfrastructureConfigurationError):
    """Raised when a source converter is registered under a mismatched source type."""


class SourceValueConverterNotRegisteredError(InfrastructureConfigurationError):
    """Raised when no source value converter exists for a source data type."""


class MockGeneratorNotRegisteredError(InfrastructureConfigurationError):
    """Raised when no mock generator exists for a source data type."""


class UnsupportedOutputDataTypeError(InfrastructureConfigurationError):
    """Raised when DDL mapping has no representation for an output data type."""


class SchemaValidationError(ValueError):
    """Raised when input schema definition is invalid."""
