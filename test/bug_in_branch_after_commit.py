#!/usr/bin/env python2
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB import versioning
from pyspatialite import dbapi2
from versioningDB.sp_versioning import spVersioning
from versioningDB.utils import Db
import os
import tempfile

PGUSER = 'postgres'
HOST = '127.0.0.1'

pg_conn_info = "dbname=epanet_test_db host="+HOST+" user="+PGUSER

if __name__ == "__main__":
    spversioning = spVersioning()
    test_data_dir = os.path.dirname(os.path.realpath(__file__))
    tmp_dir = tempfile.gettempdir()


    # create the test database
    os.system("dropdb --if-exists -h " + HOST + " -U "+PGUSER+" epanet_test_db")
    os.system("createdb -h " + HOST + " -U "+PGUSER+" epanet_test_db")
    os.system("psql -h " + HOST + " -U "+PGUSER+" epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql -h " + HOST + " -U "+PGUSER+" epanet_test_db -f "+test_data_dir+"/epanet_test_db_unversioned.sql")

    versioning.historize(pg_conn_info,"epanet")

    # try the update
    wc = tmp_dir+"/bug_in_branch_after_commit_wc.sqlite"
    if os.path.isfile(wc): os.remove(wc) 
    spversioning.checkout(pg_conn_info, ['epanet_trunk_rev_head.junctions', 'epanet_trunk_rev_head.pipes'], wc)

    scur = Db( dbapi2.connect( wc ) )

    scur.execute("SELECT * FROM pipes")
    scur.execute("UPDATE pipes_view SET length = 1 WHERE OGC_FID = 1")
    scur.commit()

    spversioning.commit([wc, pg_conn_info],'test' )

    versioning.add_branch(pg_conn_info,"epanet","mybranch","add 'branch")
