# -*- coding: utf-8 -*-
"""Distutils setup file, used to install or test 'sparksteps'."""
import textwrap

from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

setup(
    name='sparksteps',
    description='Workflow tool to launch Spark jobs on AWS EMR',
    long_description=readme,
    packages=find_packages(exclude=['tests', 'examples', 'bootstrap']),
    use_scm_version=True,
    author='Kamil Sindi',
    author_email='kamil@jwplayer.com',
    url='https://github.com/jwplayer/sparksteps',
    keywords='aws emr pyspark spark boto'.split(),
    license='Apache License 2.0',
    install_requires=[
        'boto3>=1.3.1',
        'beautifulsoup4>=4.4.1',
        'six>=1.10.0'
    ],
    setup_requires=[
        'pytest-runner',
        'setuptools_scm',
        'sphinx_rtd_theme',
    ],
    tests_require=[
        'pytest',
        'pytest-flake8',
        'moto',
    ],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'sparksteps=sparksteps.__main__:main'
        ]
    },
    classifiers=textwrap.dedent("""
        Development Status :: 4 - Beta
        Intended Audience :: Developers
        License :: OSI Approved :: Apache Software License
        Environment :: Console
        Programming Language :: Python :: 2
        Programming Language :: Python :: 2.7
        Programming Language :: Python :: 3
        Programming Language :: Python :: 3.4
        Programming Language :: Python :: 3.5
        Programming Language :: Python :: 3.6
        """).strip().splitlines(),
)
