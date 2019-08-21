import sys
import contextlib


@contextlib.contextmanager
def temporary_sys_path(*paths):
    try:
        original_paths = list(sys.path)
        sys.path.extend(paths)
        yield
    finally:
        sys.path = original_paths
