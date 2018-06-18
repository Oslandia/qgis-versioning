#!/usr/bin/env python2
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB.spatialite import spVersioning
import os
import shutil
import tempfile

PGUSER = 'postgres'
HOST = '127.0.0.1'

pg_conn_info = "dbname=epanet_test_db host="+HOST+" user="+PGUSER

def test():
    spversioning = spVersioning()
    
    test_data_dir = os.path.dirname(os.path.realpath(__file__))
    tmp_dir = tempfile.gettempdir()

    # create the test database
    os.system("dropdb --if-exists -h " + HOST + " -U "+PGUSER+" epanet_test_db")
    os.system("createdb -h " + HOST + " -U "+PGUSER+" epanet_test_db")
    os.system("psql -h " + HOST + " -U "+PGUSER+" epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql -h " + HOST + " -U "+PGUSER+" epanet_test_db -f "+test_data_dir+"/issue287_pg_dump.sql")

    # try the update
    shutil.copyfile(test_data_dir+"/issue287_wc.sqlite", tmp_dir+"/issue287_wc.sqlite")
    spversioning.update([ tmp_dir+"/issue287_wc.sqlite", pg_conn_info] )
    spversioning.commit([tmp_dir+"/issue287_wc.sqlite", pg_conn_info], "test message")

if __name__ == "__main__":
    test()

