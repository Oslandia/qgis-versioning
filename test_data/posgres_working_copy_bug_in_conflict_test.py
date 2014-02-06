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

for resolution in ['theirs','mine']:
    os.system("dropdb epanet_test_db")
    os.system("createdb epanet_test_db")
    os.system("psql epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql epanet_test_db -f "+test_data_dir+"/../html/epanet_test_db.sql")

    pcur = versioning_base.Db(psycopg2.connect("dbname=epanet_test_db"))

    tables = ['epanet_trunk_rev_head.junctions', 'epanet_trunk_rev_head.pipes']
    versioning_base.pg_checkout("dbname=epanet_test_db",tables, "wc1")
    versioning_base.pg_checkout("dbname=epanet_test_db",tables, "wc2")

    pcur.execute("UPDATE wc1.pipes_view SET length = 4 WHERE hid = 1")
    pcur.commit()
    prtTab( pcur, "wc1.pipes_diff")
    versioning_base.pg_commit("dbname=epanet_test_db","wc1","msg1")

    pcur.execute("UPDATE wc2.pipes_view SET length = 5 WHERE hid = 1")
    pcur.commit()
    prtTab( pcur, "wc2.pipes_diff")
    versioning_base.pg_update("dbname=epanet_test_db","wc2")
    prtTab( pcur, "wc2.pipes_diff")
    prtTab( pcur, "wc2.pipes_conflicts")

    pcur.execute("SELECT COUNT(*) FROM wc2.pipes_conflicts WHERE origin = 'mine'")
    assert( 1 == pcur.fetchone()[0] )
    pcur.execute("SELECT COUNT(*) FROM wc2.pipes_conflicts WHERE origin = 'theirs'")
    assert( 1 == pcur.fetchone()[0] )

    pcur.execute("DELETE FROM wc2.pipes_conflicts WHERE origin = '"+resolution+"'")
    prtTab( pcur, "wc2.pipes_conflicts")

    pcur.execute("SELECT COUNT(*) FROM wc2.pipes_conflicts")
    assert( 0 == pcur.fetchone()[0] )
    pcur.close()


