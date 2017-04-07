# coding=UTF-8
'''
To run the test:

  python -m qgis_versioning.test

All files must be in the 'test' directory and end with the string '_test.py'
'''

from __future__ import absolute_import

from . import test
import sys

if __name__=="__main__":

    test(len(sys.argv) == 2 and sys.argv[1] == '-v')
    exit(0)
