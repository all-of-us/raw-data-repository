import argparse
import importlib
import inspect
import logging
import sys
import time
import clock
import random
import string

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


def update_aes_key(current_key):
  """
  Generate a new key if there is no existing key, otherwise try to generate a new key roughly
  every six months.
  :param current_key: AES key from project config
  :return: key
  """
  month = clock.CLOCK.now().month
  new_key = '{0}{1}'.format(month if month < 10 else 9,
              ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(15)))

  if current_key is not None:
    try:
      cm = int(current_key[:1])
      if month < 7 and cm < 7:
        return current_key
      if month > 6 and cm > 6:
        return current_key
    except ValueError:
      pass

  return new_key
