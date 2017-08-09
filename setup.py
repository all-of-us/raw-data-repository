"""A setuptools based module for PIP installation."""
# Docs/example setup.py: https://github.com/pypa/sampleproject/blob/master/setup.py

import os
from setuptools import setup


base_dir = os.path.abspath(os.path.dirname(__file__))
rdr_common_dir = os.path.join(base_dir, 'rdr_common')
rdr_client_dir = os.path.join(base_dir, 'rdr_client')
with open(os.path.join(rdr_common_dir, 'README.md')) as readme:
  readme_contents = readme.read()
with open(os.path.join(rdr_client_dir, 'requirements.txt')) as requirements:
  requirements_list = [l.strip() for l in requirements.readlines()]


setup(
    # This is what people 'pip install'.
    name='all-of-us-rdr',

    long_description=readme_contents,
    url='https://github.com/vanderbilt/pmi-data',

    # These packages may be imported after the egg is installed.
    packages=['rdr_common', 'rdr_client'],

    install_requires=requirements_list,
)
