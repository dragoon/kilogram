#!/usr/bin/env python
import os.path

from setuptools import setup, find_packages
import stackexchange_utils as se

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='stackexchange-utils',
    version=se.__version__,
    author=se.__author__,
    author_email=se.__email__,
    maintainer=se.__maintainer__,
    maintainer_email=se.__email__,
    url=se.__url__,
    download_url=se.__url__,

    description=se.__summary__,
    long_description = read('README.md'),

    license=se.__license__,
    packages=find_packages(),

    requires=['lxml (>=3.0)', 'unicodecsv'],

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Topic :: Text Processing',
        'Topic :: Scientific/Engineering :: Information Analysis',
        ],
    zip_safe = True
    )
