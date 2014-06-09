#!/usr/bin/env python
import os.path

from setuptools import setup, find_packages
import ngram_utils as module_base

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='ngram-utils',
    version=module_base.__version__,
    author=module_base.__author__,
    author_email=module_base.__email__,
    maintainer=module_base.__maintainer__,
    maintainer_email=module_base.__email__,
    url=module_base.__url__,
    download_url=module_base.__url__,

    description=module_base.__summary__,
    long_description = read('README.md'),

    license=module_base.__license__,
    packages=find_packages(),

    requires=['pymongo (>=2.7)', 'thrift (>=0.9)', 'nltk (>=2.0)'],

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
