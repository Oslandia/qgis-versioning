#!/usr/bin/env python2
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB import versioning 
from pyspatialite import dbapi2
from versioningDB.spatialite import spVersioning
from versioningDB.utils import Db
import psycopg2
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
    os.system("psql -h " + HOST + " -U "+PGUSER+" epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")

    # try the update
    wc = [tmp_dir+"/issue437_wc0.sqlite", tmp_dir+"/issue437_wc1.sqlite"]
    for f in wc:
        if os.path.isfile(f): os.remove(f) 
        spversioning.checkout(pg_conn_info, ['epanet_trunk_rev_head.junctions', 'epanet_trunk_rev_head.pipes'], f)

    scur = []
    for f in wc: scur.append(Db( dbapi2.connect( f ) ))

    scur[0].execute("INSERT INTO pipes_view(id, start_node, end_node, GEOMETRY) VALUES ('2','1','2',GeomFromText('LINESTRING(1 1,0 1)',2154))")
    scur[0].execute("INSERT INTO pipes_view(id, start_node, end_node, GEOMETRY) VALUES ('3','1','2',GeomFromText('LINESTRING(1 -1,0 1)',2154))")
    scur[0].commit()


    spversioning.commit( [wc[0], pg_conn_info], 'commit 1 wc0')
    spversioning.update( [wc[1], pg_conn_info] )

    scur[0].execute("UPDATE pipes_view SET length = 1")
    scur[0].commit()
    scur[1].execute("UPDATE pipes_view SET length = 2")
    scur[1].execute("UPDATE pipes_view SET length = 3")
    scur[1].commit()

    spversioning.commit( [wc[0], pg_conn_info ], "commit 2 wc0" )
    scur[0].execute("SELECT OGC_FID,length,trunk_rev_begin,trunk_rev_end,trunk_parent,trunk_child FROM pipes")
    print '################'
    for r in scur[0].fetchall():
        print r

    scur[0].execute("UPDATE pipes_view SET length = 2")
    scur[0].execute("DELETE FROM pipes_view WHERE OGC_FID = 6")
    scur[0].commit()
    spversioning.commit( [wc[0], pg_conn_info ], "commit 3 wc0" )

    scur[0].execute("SELECT OGC_FID,length,trunk_rev_begin,trunk_rev_end,trunk_parent,trunk_child FROM pipes")
    print '################'
    for r in scur[0].fetchall():
        print r

    spversioning.update( [wc[1], pg_conn_info] )

    scur[1].execute("SELECT OGC_FID,length,trunk_rev_begin,trunk_rev_end,trunk_parent,trunk_child FROM pipes_diff")
    print '################ diff'
    for r in scur[1].fetchall():
        print r

    scur[1].execute("SELECT conflict_id FROM pipes_conflicts")
    assert( len(scur[1].fetchall()) == 6 ) # there must be conflicts

    scur[1].execute("SELECT conflict_id,origin,action,OGC_FID,trunk_parent,trunk_child FROM pipes_conflicts")
    print '################'
    for r in scur[1].fetchall():
        print r

    scur[1].execute("DELETE FROM pipes_conflicts WHERE origin='theirs' AND conflict_id=1")
    scur[1].commit()
    scur[1].execute("SELECT conflict_id FROM pipes_conflicts")
    assert( len(scur[1].fetchall()) == 4 ) # there must be two removed entries

    scur[1].execute("SELECT conflict_id,origin,action,OGC_FID,trunk_parent,trunk_child FROM pipes_conflicts")
    print '################'
    for r in scur[1].fetchall():
        print r

    scur[1].execute("DELETE FROM pipes_conflicts WHERE origin='mine' AND OGC_FID = 11")
    scur[1].execute("DELETE FROM pipes_conflicts WHERE origin='theirs'")
    scur[1].commit()
    scur[1].execute("SELECT conflict_id FROM pipes_conflicts")
    assert( len(scur[1].fetchall()) == 0 ) # there must be no conflict


    scur[1].execute("SELECT OGC_FID,length,trunk_rev_begin,trunk_rev_end,trunk_parent,trunk_child FROM pipes")
    print '################'
    for r in scur[1].fetchall():
        print r

if __name__ == "__main__":
    test()
