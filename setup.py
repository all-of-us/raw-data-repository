"""A setuptools based module for PIP installation."""
# Docs/example setup.py: https://github.com/pypa/sampleproject/blob/master/setup.py

import os

from setuptools import setup, find_packages

__VERSION__ = "1.60.6"

base_dir = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(base_dir, "README.md")) as readme:
    readme_contents = readme.read()

with open("requirements.txt") as requirements:
    requirements_list = [l.strip() for l in requirements.readlines()]

setup(
    # This is what people 'pip install'.
    name="all-of-us-rdr",
    version=__VERSION__,
    long_description=readme_contents,
    url="https://github.com/all-of-us/raw-data-repository",
    # These packages may be imported after the egg is installed.
    packages=find_packages(exclude=['tests']),
    install_requires=requirements_list,
    entry_points={
        'console_scripts': [
            'rtool = rdr_service.tools.__main__:run',
            'rclient = rdr_service.client.__main__:run',
        ],
    },
)
