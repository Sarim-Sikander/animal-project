from typing import Any, Dict, Optional


class AnimalETLException(Exception):
    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        super().__init__(self.message)


class ExternalAPIException(AnimalETLException):
    pass


class DataTransformationException(AnimalETLException):
    pass


class ValidationException(AnimalETLException):
    pass
