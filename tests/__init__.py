import os


def resolve_filename(filename):
    return os.path.join(os.path.dirname(__file__), filename)
