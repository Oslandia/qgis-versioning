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

def prtHid( cur, tab ):
    print "--- ",tab," ---"
    pcur.execute("SELECT hid FROM "+tab)
    for [r] in pcur.fetchall(): print r

test_data_dir = os.path.dirname(os.path.realpath(__file__))

# create the test database

os.system("dropdb epanet_test_db")
os.system("createdb epanet_test_db")
os.system("psql epanet_test_db -c 'CREATE EXTENSION postgis'")
os.system("psql epanet_test_db -f "+test_data_dir+"/../html/epanet_test_db.sql")

# chechout
versioning_base.pg_checkout("dbname=epanet_test_db",['epanet_trunk_rev_head.junctions','epanet_trunk_rev_head.pipes'], "epanet_working_copy")

pcur = versioning_base.Db(psycopg2.connect("dbname=epanet_test_db"))

pcur.execute("UPDATE epanet_working_copy.pipes_view SET length = 4 WHERE hid = 1")
prtTab(pcur, 'epanet_working_copy.pipes_diff')

prtHid( pcur, 'epanet_working_copy.pipes_view')
pcur.execute("SElECT COUNT(hid) FROM epanet_working_copy.pipes_view")
assert( 1 == pcur.fetchone()[0] )
