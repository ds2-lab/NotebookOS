import os

def get_username():
    """
    Get and return the username of the current user.

    This should work on both Windows and Linux systems (with WSL/WSL2 treated as Linux).

    :return: the username of the current user.
    """
    if os.name == 'nt':
        try:
            return os.environ['USERNAME']
        except KeyError:
            return os.getlogin()
    else:
        try:
            return os.environ['USER']
        except KeyError:
            import pwd

            return pwd.getpwuid(os.getuid())[0]