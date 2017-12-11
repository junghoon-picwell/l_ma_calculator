import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def get_version_number():
    version_file = open(os.path.join(ROOT_DIR, 'VERSION'))
    return version_file.read().strip()
