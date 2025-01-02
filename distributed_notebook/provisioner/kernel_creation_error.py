from typing import Optional

import grpc


class KernelCreationError(Exception):
    def __init__(
            self,
            message: str,
            grpc_status_code: Optional[grpc.StatusCode] = None,
            grpc_debug_error_str: Optional[str] = None,
            grpc_details: Optional[str] = None
    ):
        super().__init__(message)

        self.error_message: str = message

        if grpc_status_code is not None:
            num, code = grpc_status_code.value

            self.grpc_status_code: str = code
            self.grpc_status_number: int = num

        self.grpc_debug_error_str: Optional[str] = grpc_debug_error_str
        self.grpc_details: Optional[str] = grpc_details

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return (f"KernelCreationError gRPC {self.grpc_status_code} ({self.grpc_status_number}): "
                f"msg = {self.error_message} "
                f"debug = {self.grpc_debug_error_str}, "
                f"details = {self.grpc_details}")