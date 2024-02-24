#!/usr/bin/env python
from setuptools import setup, find_packages
import codecs
import os

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

VERSION = '1.0.2'
DESCRIPTION = 'Bulk records add into Cassandra or ScyllaDB.'
LONG_DESCRIPTION =long_description

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
        "pandas",
        "numpy",
        "pyarrow",
        "cassandra-driver==3.29.0"
    ],
)
