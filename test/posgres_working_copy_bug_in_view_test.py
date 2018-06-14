#!/usr/bin/env python2
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB.pg_versioning import pgVersioning
from versioningDB.utils import Db
import psycopg2
import os
import shutil

PGUSER = 'postgres'
HOST = '127.0.0.1'

pg_conn_info = "dbname=epanet_test_db host="+HOST+" user="+PGUSER

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

def test():
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    versioning = pgVersioning()
    # create the test database

    os.system("dropdb --if-exists -h " + HOST + " -U "+PGUSER+" epanet_test_db")
    os.system("createdb -h " + HOST + " -U "+PGUSER+" epanet_test_db")
    os.system("psql -h " + HOST + " -U "+PGUSER+" epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql -h " + HOST + " -U "+PGUSER+" epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")

    # chechout
    versioning.checkout(pg_conn_info,['epanet_trunk_rev_head.junctions','epanet_trunk_rev_head.pipes'], "epanet_working_copy")

    pcur = Db(psycopg2.connect(pg_conn_info))

    pcur.execute("UPDATE epanet_working_copy.pipes_view SET length = 4 WHERE pid = 1")
    prtTab(pcur, 'epanet_working_copy.pipes_diff')

    prtHid( pcur, 'epanet_working_copy.pipes_view')
    pcur.execute("SElECT COUNT(pid) FROM epanet_working_copy.pipes_view")
    assert( 1 == pcur.fetchone()[0] )

if __name__ == "__main__":
    test()
