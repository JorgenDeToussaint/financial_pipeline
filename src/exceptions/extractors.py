class ExtractorError(Exception):
    pass

class RateLimitError(ExtractorError):
    def __init__(self, message="Too Many Requests", retry_after=None):
        self.retry_after = retry_after
        super().__init__(message)

class AuthError(ExtractorError):
    pass

class ServerError(ExtractorError):
    pass

class DataIntegrityError(ExtractorError):
    pass