#!/usr/bin/env python
"""N-gram processing utils"""
import os.path
from setuptools import setup, find_packages

__author__      = 'Roman Prokofyev'
__license__     = 'Apache License 2.0'
__version__     = '0.1'
__maintainer__  = __author__
__email__       = 'roman.prokofyev@gmail.com'
__url__         = 'https://github.com/dragoon/kilogram/'
__summary__     = __doc__

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='kilogram',
    version=__version__,
    author=__author__,
    author_email=__email__,
    maintainer=__maintainer__,
    maintainer_email=__email__,
    url=__url__,
    download_url=__url__,

    description=__summary__,
    long_description=read('README.md'),

    license=__license__,
    packages=find_packages(),

    install_requires=['lxml>=3.0', 'unicodecsv', 'pymongo>=2.7', 'thrift>=0.9', 'nltk<2.1',
                      'pandas>=0.14'],
    dependency_links=[
        'https://github.com/dragoon/pyutils/tarball/master#egg=pyutils-dev',
    ],
    scripts=['kilogram/dataset/stackexchange/se_parse_edit_history.py',
             'kilogram/dataset/wikipedia/wiki_parse_edit_history.py',
             'kilogram/dataset/conll/conll_sgml_parse.py',
             'kilogram/dataset/fce/fce_parse_edit_history.py'],

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Topic :: Text Processing',
        'Topic :: Scientific/Engineering :: Information Analysis',
        ],
    zip_safe=True
    )
