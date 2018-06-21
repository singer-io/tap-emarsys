#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-emarsys",
    version="0.2.0",
    description="Singer.io tap for extracting data from the Emarsys API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_emarsys"],
    install_requires=[
        "singer-python>=5.1.1",
        "pendulum",
        "ratelimit",
        "backoff",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-emarsys=tap_emarsys:main
    """,
    packages=find_packages(),
    package_data = {
        "schemas": ["tap_emarsys/schemas/*.json"]
    },
    include_package_data=True
)
