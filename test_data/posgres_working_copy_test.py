#!/usr/bin/python
import versioning_base
import psycopg2
import os
import shutil

def prtTab( cur, tab ):
    print "--- ",tab," ---"
    pcur.execute("SELECT hid, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child, length FROM "+tab)
    for r in pcur.fetchall():
        t = []
        for i in r: t.append(str(i))
        print '\t| '.join(t)

test_data_dir = os.path.dirname(os.path.realpath(__file__))

# create the test database

os.system("dropdb epanet_test_db")
os.system("createdb epanet_test_db")
os.system("psql epanet_test_db -c 'CREATE EXTENSION postgis'")
os.system("psql epanet_test_db -f "+test_data_dir+"/../html/epanet_test_db.sql")

# chechout
versioning_base.pg_checkout("dbname=epanet_test_db",['epanet_trunk_rev_head.junctions','epanet_trunk_rev_head.pipes'], "epanet_working_copy")

versioning_base.pg_checkout("dbname=epanet_test_db",['epanet_trunk_rev_head.junctions','epanet_trunk_rev_head.pipes'], "epanet_working_copy_cflt")

pcur = versioning_base.Db(psycopg2.connect("dbname=epanet_test_db"))


pcur.execute("INSERT INTO epanet_working_copy.pipes_view(id, start_node, end_node, geom) VALUES ('2','1','2',ST_GeometryFromText('LINESTRING(1 1,0 1)',2154))")
pcur.execute("INSERT INTO epanet_working_copy.pipes_view(id, start_node, end_node, geom) VALUES ('3','1','2',ST_GeometryFromText('LINESTRING(1 -1,0 1)',2154))")
pcur.commit()

pcur.execute("SELECT hid FROM epanet_working_copy.pipes_view")
assert( len(pcur.fetchall()) == 3 )
pcur.execute("SELECT hid FROM epanet_working_copy.pipes_diff")
assert( len(pcur.fetchall()) == 2 )
pcur.execute("SELECT hid FROM epanet.pipes")
assert( len(pcur.fetchall()) == 1 )


prtTab(pcur, 'epanet.pipes')
prtTab(pcur, 'epanet_working_copy.pipes_diff')
pcur.execute("UPDATE epanet_working_copy.pipes_view SET length = 4 WHERE hid = 1")
prtTab(pcur, 'epanet_working_copy.pipes_diff')
pcur.execute("UPDATE epanet_working_copy.pipes_view SET length = 5 WHERE hid = 4")
prtTab(pcur, 'epanet_working_copy.pipes_diff')

pcur.execute("DELETE FROM epanet_working_copy.pipes_view WHERE hid = 4")
pcur.commit()
prtTab(pcur, 'epanet_working_copy.pipes_diff')

versioning_base.pg_commit("dbname=epanet_test_db", "epanet_working_copy","test commit msg")
prtTab(pcur, 'epanet.pipes')

pcur.execute("SELECT trunk_rev_end FROM epanet.pipes WHERE hid = 1")
assert( 1 == pcur.fetchone()[0] )
pcur.execute("SELECT COUNT(*) FROM epanet.pipes WHERE trunk_rev_begin = 2")
assert( 2 == pcur.fetchone()[0] )


# modify the second working copy to create conflict
prtTab(pcur, 'epanet.pipes')
pcur.execute("UPDATE epanet_working_copy_cflt.pipes_view SET length = 8 ")
pcur.commit()
prtTab(pcur, 'epanet_working_copy_cflt.pipes_diff')
pcur.execute("INSERT INTO epanet_working_copy_cflt.pipes_view(id, start_node, end_node, geom) VALUES ('3','1','2',ST_GeometryFromText('LINESTRING(1 -1,0 1)',2154))")
pcur.commit()
prtTab(pcur, 'epanet_working_copy_cflt.pipes_diff')
versioning_base.pg_update( "dbname=epanet_test_db", "epanet_working_copy_cflt" )
prtTab(pcur, 'epanet_working_copy_cflt.pipes_diff')

pcur.execute("SELECT COUNT(*) FROM epanet_working_copy_cflt.pipes_conflicts")
assert( 2 == pcur.fetchone()[0] )

prtTab(pcur, 'epanet_working_copy_cflt.pipes_conflicts')

pcur.execute("DELETE FROM epanet_working_copy_cflt.pipes_conflicts WHERE origin = 'theirs'")
pcur.commit()
prtTab(pcur, 'epanet_working_copy_cflt.pipes_diff')
prtTab(pcur, 'epanet_working_copy_cflt.pipes_conflicts')

versioning_base.pg_commit("dbname=epanet_test_db", "epanet_working_copy_cflt","second test commit msg")

