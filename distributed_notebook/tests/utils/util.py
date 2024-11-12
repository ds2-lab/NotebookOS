import os

def extract_last_two_parts(path):
    # Split the path into parts
    parts = path.split(os.sep)

    if len(parts) == 0:
        return "./"
    elif len(parts) == 1:
        return parts[0]
    elif len(parts) == 2:
        return path

    # Join the last two parts
    return "../" + os.path.join(parts[-2], parts[-1])