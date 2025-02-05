from typing import Optional

class InvalidKeyError(Exception):
    def __init__(self, message, key:Optional[str] = None):
        super().__init__(message)

        self.key = key