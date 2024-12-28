"""
Custom exceptions
"""

class PackageError(Exception):
    """Base exception class."""
    pass

class CalculationError(PackageError):
    """Calculation error."""
    pass

class ConversionError(PackageError):
    """Conversion error."""
    pass

class ProcessingError(PackageError):
    """Processing error."""
    pass

class ValidationError(PackageError):
    """Validation error."""
    pass

class FormattingError(PackageError):
    """Formatting error."""
    pass

class RetryError(PackageError):
    """Retry error."""
    pass 