class ValidationError(Exception):
    """This kind of validation error is thrown whenever an :class:`Application` or :class:`Collection` is
    misconfigured."""

class ParseError(Exception):
    "Raised when a method signature cannot be parsed."
