#!/usr/bin/env python2
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB import versioning
import psycopg2
import os
import shutil


def prtTab( cur, tab ):
    print "--- ",tab," ---"
    cur.execute("SELECT pid, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child, length FROM "+tab)
    for r in cur.fetchall():
        t = []
        for i in r: t.append(str(i))
        print '\t| '.join(t)

def prtHid( cur, tab ):
    print "--- ",tab," ---"
    cur.execute("SELECT pid FROM "+tab)
    for [r] in cur.fetchall(): print r

def test(host, pguser):
    pg_conn_info = "dbname=epanet_test_db host=" + host + " user=" + pguser
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    # create the test database

    for resolution in ['theirs','mine']:
        os.system("dropdb --if-exists -h " + host + " -U "+pguser+" epanet_test_db")
        os.system("createdb -h " + host + " -U "+pguser+" epanet_test_db")
        os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -c 'CREATE EXTENSION postgis'")
        os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")

        pcur = versioning.Db(psycopg2.connect(pg_conn_info))

        tables = ['epanet_trunk_rev_head.junctions', 'epanet_trunk_rev_head.pipes']
        pgversioning1 = versioning.pgLocal(pg_conn_info, 'wc1')
        pgversioning2 = versioning.pgLocal(pg_conn_info, 'wc2')
        pgversioning1.checkout(tables)
        pgversioning2.checkout(tables)
        print "checkout done"

        pcur.execute("UPDATE wc1.pipes_view SET length = 4 WHERE pid = 1")
        prtTab( pcur, "wc1.pipes_diff")
        pcur.commit()
        #pcur.close()
        pgversioning1.commit("msg1")

        #pcur = versioning.Db(psycopg2.connect(pg_conn_info))

        print "commited"
        pcur.execute("UPDATE wc2.pipes_view SET length = 5 WHERE pid = 1")
        prtTab( pcur, "wc2.pipes_diff")
        pcur.commit()
        pgversioning2.update()
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


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python2 versioning_base_test.py host pguser")
    else:
        test(*sys.argv[1:])