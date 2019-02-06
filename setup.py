#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

with open('requirements.txt') as fp:
    install_requires = fp.read()

with open('requirements_dev.txt') as fp:
    install_test_requires = fp.read()


setup(
    name='meucandidato-dags',
    version='0.0.1',
    description="DAGs of fetch data to project",
    long_description=readme + '\n\n' + history,
    author="Gilson Filho",
    author_email='me@gilsondev.in',
    url='https://github.com/meucandidato/meucandidato-dags',
    packages=find_packages(include=['meucandidato_dags']),
    include_package_data=True,
    install_requires=install_requires,
    zip_safe=False,
    keywords='dags, data, meucandidato, politics',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    test_require=install_test_requires
)
