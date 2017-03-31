#!/usr/bin/python
from .. import versioning
from pyspatialite import dbapi2
import psycopg2
import os
import shutil
import tempfile

if __name__ == "__main__":
    test_data_dir = os.path.dirname(os.path.realpath(__file__))
    tmp_dir = tempfile.gettempdir()

    # create the test database

    os.system("dropdb epanet_test_db")
    os.system("createdb epanet_test_db")
    os.system("psql epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")

    # try the update
    wc = tmp_dir+"/issue358_wc.sqlite"
    if os.path.isfile(wc): os.remove(wc) 
    versioning.checkout("dbname=epanet_test_db", ['epanet_trunk_rev_head.junctions', 'epanet_trunk_rev_head.pipes'], wc)

    scur = versioning.Db( dbapi2.connect( wc ) )

    scur.execute("SELECT * FROM pipes")
    assert( len(scur.fetchall()) == 1 )
    scur.execute("UPDATE pipes_view SET length = 1 WHERE OGC_FID = 1")
    scur.execute("SELECT * FROM pipes")
    assert( len(scur.fetchall()) == 2 )
    scur.execute("UPDATE pipes_view SET length = 2 WHERE OGC_FID = 2")
    scur.execute("SELECT * FROM pipes")
    assert( len(scur.fetchall()) == 2 )
