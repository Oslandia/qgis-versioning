# coding=UTF-8
'''
To run the test:

  python -m qgis_versioning.test [-v]

All files must be in the 'test' directory and end with the string '_test.py'
'''

from __future__ import absolute_import

from . import test

if __name__=="__main__":

    test()
    exit(0)
