#!/usr/bin/env python2
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB import versioning 
from pyspatialite import dbapi2
import psycopg2
import os
import shutil
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
    wc = [os.path.join(tmp_dir,"issue437_wc0.sqlite"), os.path.join(tmp_dir,"issue437_wc1.sqlite")]
    spversioning0 = versioning.spatialite(wc[0], pg_conn_info)
    spversioning1 = versioning.spatialite(wc[1], pg_conn_info)
    for i, f in enumerate(wc):
        if os.path.isfile(f): os.remove(f) 
        sp = spversioning0 if i == 0 else spversioning1
        sp.checkout(['epanet_trunk_rev_head.junctions', 'epanet_trunk_rev_head.pipes'])

    scur = []
    for f in wc: scur.append(versioning.Db( dbapi2.connect( f ) ))

    scur[0].execute("INSERT INTO pipes_view(id, start_node, end_node, GEOMETRY) VALUES ('2','1','2',GeomFromText('LINESTRING(1 1,0 1)',2154))")
    scur[0].execute("INSERT INTO pipes_view(id, start_node, end_node, GEOMETRY) VALUES ('3','1','2',GeomFromText('LINESTRING(1 -1,0 1)',2154))")
    scur[0].commit()


    spversioning0.commit( 'commit 1 wc0')
    spversioning1.update(  )

    scur[0].execute("UPDATE pipes_view SET length = 1")
    scur[0].commit()
    scur[1].execute("UPDATE pipes_view SET length = 2")
    scur[1].execute("UPDATE pipes_view SET length = 3")
    scur[1].commit()

    spversioning0.commit( "commit 2 wc0" )
    scur[0].execute("SELECT OGC_FID,length,trunk_rev_begin,trunk_rev_end,trunk_parent,trunk_child FROM pipes")
    print('################')
    for r in scur[0].fetchall():
        print(r)

    scur[0].execute("UPDATE pipes_view SET length = 2")
    scur[0].execute("DELETE FROM pipes_view WHERE OGC_FID = 6")
    scur[0].commit()
    spversioning0.commit( "commit 3 wc0" )

    scur[0].execute("SELECT OGC_FID,length,trunk_rev_begin,trunk_rev_end,trunk_parent,trunk_child FROM pipes")
    print('################')
    for r in scur[0].fetchall():
        print(r)

    spversioning1.update(  )

    scur[1].execute("SELECT OGC_FID,length,trunk_rev_begin,trunk_rev_end,trunk_parent,trunk_child FROM pipes_diff")
    print('################ diff')
    for r in scur[1].fetchall():
        print(r)

    scur[1].execute("SELECT conflict_id FROM pipes_conflicts")
    assert( len(scur[1].fetchall()) == 6 ) # there must be conflicts

    scur[1].execute("SELECT conflict_id,origin,action,OGC_FID,trunk_parent,trunk_child FROM pipes_conflicts")
    print('################')
    for r in scur[1].fetchall():
        print(r)

    scur[1].execute("DELETE FROM pipes_conflicts WHERE origin='theirs' AND conflict_id=1")
    scur[1].commit()
    scur[1].execute("SELECT conflict_id FROM pipes_conflicts")
    assert( len(scur[1].fetchall()) == 4 ) # there must be two removed entries

    scur[1].execute("SELECT conflict_id,origin,action,OGC_FID,trunk_parent,trunk_child FROM pipes_conflicts")
    print('################')
    for r in scur[1].fetchall():
        print(r)

    scur[1].execute("DELETE FROM pipes_conflicts WHERE origin='mine' AND OGC_FID = 11")
    scur[1].execute("DELETE FROM pipes_conflicts WHERE origin='theirs'")
    scur[1].commit()
    scur[1].execute("SELECT conflict_id FROM pipes_conflicts")
    assert( len(scur[1].fetchall()) == 0 ) # there must be no conflict


    scur[1].execute("SELECT OGC_FID,length,trunk_rev_begin,trunk_rev_end,trunk_parent,trunk_child FROM pipes")
    print('################')
    for r in scur[1].fetchall():
        print(r)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python2 versioning_base_test.py host pguser")
    else:
        test(*sys.argv[1:])
