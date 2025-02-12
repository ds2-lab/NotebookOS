from typing import List, ByteString

def split_bytes_buffer(buffer: bytes, chunk_size: int = 128 * 1024 * 1024) -> List[ByteString]:
    """
    Splits a large bytes buffer into 128MB chunks (or a smaller final chunk if needed).

    :param buffer: The bytes buffer to split.
    :param chunk_size: The maximum chunk size (default: 128MB).

    :return: A list of byte chunks.
    """
    return [buffer[i:i + chunk_size] for i in range(0, len(buffer), chunk_size)]