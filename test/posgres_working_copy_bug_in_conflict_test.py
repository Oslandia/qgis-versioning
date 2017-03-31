#!/usr/bin/python
from .. import versioning
import psycopg2
import os
import shutil

def prtTab( cur, tab ):
    print "--- ",tab," ---"
    pcur.execute("SELECT pid, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child, length FROM "+tab)
    for r in pcur.fetchall():
        t = []
        for i in r: t.append(str(i))
        print '\t| '.join(t)

def prtHid( cur, tab ):
    print "--- ",tab," ---"
    pcur.execute("SELECT pid FROM "+tab)
    for [r] in pcur.fetchall(): print r

if __name__ == "__main__":
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    # create the test database

    for resolution in ['theirs','mine']:
        os.system("dropdb --if-exists epanet_test_db")
        os.system("createdb epanet_test_db")
        os.system("psql epanet_test_db -c 'CREATE EXTENSION postgis'")
        os.system("psql epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")

        pcur = versioning.Db(psycopg2.connect("dbname=epanet_test_db"))

        tables = ['epanet_trunk_rev_head.junctions', 'epanet_trunk_rev_head.pipes']
        versioning.pg_checkout("dbname=epanet_test_db",tables, "wc1")
        versioning.pg_checkout("dbname=epanet_test_db",tables, "wc2")
        print "checkout done"

        pcur.execute("UPDATE wc1.pipes_view SET length = 4 WHERE pid = 1")
        prtTab( pcur, "wc1.pipes_diff")
        pcur.commit()
        #pcur.close()
        versioning.pg_commit("dbname=epanet_test_db","wc1","msg1")

        #pcur = versioning.Db(psycopg2.connect("dbname=epanet_test_db"))

        print "commited"
        pcur.execute("UPDATE wc2.pipes_view SET length = 5 WHERE pid = 1")
        prtTab( pcur, "wc2.pipes_diff")
        pcur.commit()
        versioning.pg_update("dbname=epanet_test_db","wc2")
        print "updated"
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


