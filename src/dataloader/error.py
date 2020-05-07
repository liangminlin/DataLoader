class AppError(Exception):
    def __init__(self, message=None):
        self.message = str(message)

    def __str__(self):
        return self.message


class UnsupportError(AppError):
    def __init__(self, message=None):
        super().__init__(message)


class ConfigError(AppError):
    def __init__(self, message=None):
        super().__init__(message)
