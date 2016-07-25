# -*- coding: utf-8 -*-
"""Distutils setup file, used to install or test 'sparksteps'."""
import textwrap

from setuptools import setup, find_packages

setup(
    name='sparksteps',
    description='Create and add spark steps to EMR cluster',
    version='0.2.0',
    author='Kamil Sindi (JWPlayer)',
    author_email='kamil@jwplayer.com',
    classifiers=textwrap.dedent("""
        Development Status :: 3 - Alpha
        Intended Audience :: Developers
        License :: OSI Approved :: Apache Software License
        Environment :: Console
        Programming Language :: Python
        Programming Language :: Python :: 2.7
        Programming Language :: Python :: 3
        Programming Language :: Python :: 3.5
        """).strip().splitlines(),
    keywords='AWS EMR pyspark spark boto',
    license='Apache License 2.0',
    install_requires=[
        'boto3>=1.3.1',
        'beautifulsoup4>=4.4.1',
        'six>=1.10.0'
    ],
    packages=find_packages(exclude=['tests.*', 'examples', 'bootstrap']),
    include_package_data=True,
    tests_require=['pytest', 'moto'],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'sparksteps=sparksteps.main:main'
        ]
    }
)
