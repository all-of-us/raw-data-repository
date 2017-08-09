"""A setuptools based module for PIP installation."""
# Docs/example setup.py: https://github.com/pypa/sampleproject/blob/master/setup.py

import os
from setuptools import setup


with open(os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'rdr_common', 'README.md')) as readme:
  readme_contents = readme.read()


setup(
    version='0.1',

    # This is what people 'pip install'.
    name='all-of-us-rdr',

    long_description=readme_contents,
    url='https://github.com/vanderbilt/pmi-data',

    # These packages may be imported after the egg is installed.
    packages=['rdr_common', 'rdr_client'],
)
