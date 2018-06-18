#!/usr/bin/env python2
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from pyspatialite import dbapi2
from versioningDB.spatialite import spVersioning
from versioningDB.utils import Db
import os
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
    os.system("psql -h " + HOST + " -U "+PGUSER+" epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")

    # try the update
    wc = tmp_dir+"/issue358_wc.sqlite"
    if os.path.isfile(wc): os.remove(wc) 
    spversioning.checkout(pg_conn_info, ['epanet_trunk_rev_head.junctions', 'epanet_trunk_rev_head.pipes'], wc)

    scur = Db( dbapi2.connect( wc ) )

    scur.execute("SELECT * FROM pipes")
    assert( len(scur.fetchall()) == 1 )
    scur.execute("UPDATE pipes_view SET length = 1 WHERE OGC_FID = 1")
    scur.execute("SELECT * FROM pipes")
    assert( len(scur.fetchall()) == 2 )
    scur.execute("UPDATE pipes_view SET length = 2 WHERE OGC_FID = 2")
    scur.execute("SELECT * FROM pipes")
    assert( len(scur.fetchall()) == 2 )

if __name__ == "__main__":
    test()
