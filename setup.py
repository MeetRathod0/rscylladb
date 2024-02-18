#!/usr/bin/env python
from setuptools import setup
from setuptools import setup, find_packages
import codecs
import os

here = os.path.abspath(os.path.dirname(__file__))


VERSION = '0.0.1'
DESCRIPTION = 'Insert records into cassandra db.'
LONG_DESCRIPTION = 'Insert batch records from .csv in the scylladb/cassandra'

# Setting up
setup(
    name="rscylladb",
    version=VERSION,
    author="Reeya Patel and Meet Rathod",
    author_email="",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=[
        "pandas>=2.2.0",
        "numpy>=1.26.4",
        "pyarrow>=15.0.0",
        "cassandra-driver>=3.29.0"
    ],
)
