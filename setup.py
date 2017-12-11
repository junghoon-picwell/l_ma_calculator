#!/usr/bin/env python

from setuptools import setup, find_packages
from version import get_version_number

version=get_version_number()

setup(
    name='lambda_ma_calculator',
    packages=find_packages(exclude=['*.test', '*.tests']),
    version=version,
    description='Picwell Medicare Advantage Calculator',
    author='Picwell',
    author_email='dev@picwell.com',
    url='http://www.picwell.com/',
    install_requires=['boto3'],
    tests_require=[
        'pytest',
    ],
    dependency_links=[],
)
