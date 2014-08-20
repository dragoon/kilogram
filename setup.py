#!/usr/bin/env python
import os.path

from setuptools import setup, find_packages
from kilogram import __meta__ as module_base


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='kilogram',
    version=module_base.__version__,
    author=module_base.__author__,
    author_email=module_base.__email__,
    maintainer=module_base.__maintainer__,
    maintainer_email=module_base.__email__,
    url=module_base.__url__,
    download_url=module_base.__url__,

    description=module_base.__summary__,
    long_description=read('README.md'),

    license=module_base.__license__,
    packages=find_packages(),

    install_requires=['lxml>=3.0', 'unicodecsv', 'pymongo>=2.7', 'thrift>=0.9', 'nltk>=2.0'],
    dependency_links=[
        'https://github.com/dragoon/pyutils/tarball/master#egg=pyutils-dev',
    ],
    scripts=['bin/mongo/insert_to_mongo.py', 'bin/mongo/convert_to_mongo.py',
             'kilogram/dataset/stackexchange/se_parse_edit_history.py',
             'kilogram/dataset/wikipedia/wiki_parse_edit_history.py',
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
