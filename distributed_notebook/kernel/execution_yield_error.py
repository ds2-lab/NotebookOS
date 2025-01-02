class ExecutionYieldError(Exception):
    """Exception raised when execution is yielded."""

    def __init__(self, message):
        super().__init__(message)