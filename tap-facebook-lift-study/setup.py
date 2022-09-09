#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-facebook-lift-study",
    version="1.0.0",
    description="singer tap extracting facebook ads lift studies data",
    author="emile.caron@uptilab.com",
    url="https://reeport.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_facebook_lift_study"],
    install_requires=[
        "singer-python",
        "facebook-business==9.0.2",
    ],
    entry_points="""
        [console_scripts]
        tap-facebook-lift-study=tap_facebook_lift_study:main
    """,
    packages=["tap_facebook_lift_study"],
    package_data={
        "tap_facebook_lift_study": ["schemas/*.json"],
    },
    include_package_data=True,
)
