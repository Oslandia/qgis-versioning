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
    os.system("psql epanet_test_db -f "+test_data_dir+"/issue437_test_db.sql")

    # try the update
    wc = [tmp_dir+"/issue437_wc0.sqlite", tmp_dir+"/issue437_wc1.sqlite"]
    for f in wc:
        if os.path.isfile(f): os.remove(f) 
        versioning.checkout("dbname=epanet_test_db", ['epanet_trunk_rev_head.junctions', 'epanet_trunk_rev_head.pipes'], f)

    scur = []
    for f in wc: scur.append(versioning.Db( dbapi2.connect( f ) ))

    scur[0].execute("INSERT INTO pipes_view(id, start_node, end_node, GEOMETRY) VALUES ('2','1','2',GeomFromText('LINESTRING(1 1,0 1)',2154))")
    scur[0].execute("INSERT INTO pipes_view(id, start_node, end_node, GEOMETRY) VALUES ('3','1','2',GeomFromText('LINESTRING(1 -1,0 1)',2154))")
    scur[0].commit()


    versioning.commit( wc[0], 'commit 1 wc0', "dbname=epanet_test_db")
    versioning.update( wc[1], "dbname=epanet_test_db" )

    scur[0].execute("UPDATE pipes_view SET length = 1")
    scur[0].commit()
    scur[1].execute("UPDATE pipes_view SET length = 2")
    scur[1].execute("UPDATE pipes_view SET length = 3")
    scur[1].commit()

    versioning.commit( wc[0], "commit 2 wc0", "dbname=epanet_test_db" )
    scur[0].execute("SELECT OGC_FID,length,trunk_rev_begin,trunk_rev_end,trunk_parent,trunk_child FROM pipes")
    print '################'
    for r in scur[0].fetchall():
        print r

    scur[0].execute("UPDATE pipes_view SET length = 2")
    scur[0].execute("DELETE FROM pipes_view WHERE OGC_FID = 6")
    scur[0].commit()
    versioning.commit( wc[0], "commit 3 wc0", "dbname=epanet_test_db" )

    scur[0].execute("SELECT OGC_FID,length,trunk_rev_begin,trunk_rev_end,trunk_parent,trunk_child FROM pipes")
    print '################'
    for r in scur[0].fetchall():
        print r

    versioning.update( wc[1], "dbname=epanet_test_db" )

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

