#!/usr/bin/env python3
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB import versioning
from sqlite3 import dbapi2
import os
import tempfile

def test(host, pguser):
    pg_conn_info = "dbname=epanet_test_db host=" + host + " user=" + pguser
    test_data_dir = os.path.dirname(os.path.realpath(__file__))
    tmp_dir = tempfile.gettempdir()

    # create the test database
    os.system("dropdb --if-exists -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("createdb -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")

    # try the update
    wc = tmp_dir+"/issue358_wc.sqlite"
    if os.path.isfile(wc): os.remove(wc) 
    spversioning = versioning.spatialite(wc, pg_conn_info)
    spversioning.checkout(['epanet_trunk_rev_head.junctions', 'epanet_trunk_rev_head.pipes'])

    scur = versioning.Db( dbapi2.connect( wc ) )

    scur.execute("SELECT * FROM pipes")
    assert( len(scur.fetchall()) == 1 )
    scur.execute("UPDATE pipes_view SET length = 1 WHERE OGC_FID = 1")
    scur.execute("SELECT * FROM pipes")
    assert( len(scur.fetchall()) == 2 )
    scur.execute("UPDATE pipes_view SET length = 2 WHERE OGC_FID = 2")
    scur.execute("SELECT * FROM pipes")
    assert( len(scur.fetchall()) == 2 )

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 versioning_base_test.py host pguser")
    else:
        test(*sys.argv[1:])
