import argparse
import importlib
import inspect
import logging
import sys
import time


def configure_logging():
  logging.Formatter.converter = time.gmtime  # Log in UTC.
  logging.basicConfig(
      stream=sys.stdout,
      level=logging.INFO,
      format='%(asctime)s %(levelname)s: %(message)s')

def get_parser(description=None):
  """Gets an ArgumentParser, defaulting to the caller's __doc__ as the description."""
  if description is None:
    caller_frame = inspect.stack()[1]
    caller_path = caller_frame[1]
    caller_module_name = inspect.getmodulename(caller_path)
    try:
      caller_module = importlib.import_module(caller_module_name)
      doc = caller_module.__doc__
    except ImportError:
      logging.error(
          'Could not auto-detect __doc__ for parser description for module %r, '
          'derived from caller path %r.',
          caller_module_name,
          caller_path)
      doc = None
  else:
    doc = description
  return argparse.ArgumentParser(
      description=doc,
      formatter_class=argparse.RawDescriptionHelpFormatter)
