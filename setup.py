#!/usr/bin/env python
import os
import re

from setuptools import find_namespace_packages
from setuptools import setup

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()

package_name = "buenavista"
package_version = "0.3.0"

description = """Programmable Presto and Postgres Proxies"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Josh Wills",
    author_email="joshwills+bv@gmail.com",
    url="https://github.com/jwills/buenavista",
    license="Apache",
    packages=find_namespace_packages(include=["buenavista", "buenavista.*"]),
    include_package_data=True,
    install_requires=[
        "fastapi",
        "pydantic>=1.2.0,<2.0.0",
        "sqlglot",
    ],
    extras_require={
        "duckdb": ["duckdb", "pyarrow"],
        "postgres": ["psycopg", "psycopg-pool"],
    },
)
