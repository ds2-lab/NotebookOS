from typing import Dict, Any

class ElectionAbortedException(Exception):
    def __init__(self, message: str):
        super().__init__(message)

def extract_header(msg_or_header: Dict[str, Any]) -> Dict[str, Any]:
    """Given a message or header, return the header."""
    if not msg_or_header:
        return {}
    try:
        # See if msg_or_header is the entire message.
        h = msg_or_header["header"]
    except KeyError:
        try:
            # See if msg_or_header is just the header
            h = msg_or_header["msg_id"]
        except KeyError:
            raise
        else:
            h = msg_or_header
    if not isinstance(h, dict):
        h = dict(h)
    return h
