import argparse
import inspect
import logging
import sys


def configure_logging():
   logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s: %(message)s')


def get_parser(description=None):
  """Gets an ArgumentParser, defaulting to the caller's __doc__ as the description."""
  if description is None:
    doc = inspect.getmodule(inspect.stack()[1]).__doc__
  else:
    doc = description
  return argparse.ArgumentParser(
      description=doc,
      formatter_class=argparse.RawDescriptionHelpFormatter)
