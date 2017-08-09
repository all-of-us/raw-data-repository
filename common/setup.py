"""A setuptools based module for PIP installation."""
# Docs/example setup.py: https://github.com/pypa/sampleproject/blob/master/setup.py

import os
from setuptools import setup


with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'README.md')) as readme:
  readme_contents = readme.read()


setup(
    version='0.1',
    name='All of Us RDR common',
    long_description=readme_contents,
    url='https://github.com/vanderbilt/pmi-data',
    py_modules=['code_constants', 'main_util'],
)
