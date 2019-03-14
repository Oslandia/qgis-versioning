#!/usr/bin/env python3
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB import versioning
import psycopg2
import os
import shutil


def prtTab( cur, tab ):
    print("--- ",tab," ---")
    cur.execute("SELECT versioning_hid, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child, length FROM "+tab)
    for r in cur.fetchall():
        t = []
        for i in r: t.append(str(i))
        print('\t| '.join(t))

def prtHid( cur, tab ):
    print("--- ",tab," ---")
    cur.execute("SELECT versioning_hid FROM "+tab)
    for [r] in cur.fetchall(): print(r)

def test(host, pguser):
    pg_conn_info = "dbname=epanet_test_db host=" + host + " user=" + pguser
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    # create the test database
    os.system("dropdb --if-exists -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("createdb -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")
    versioning.historize("dbname=epanet_test_db host={} user={}".format(host,pguser), "epanet")

    # checkout
    pgversioning = versioning.pgServer(pg_conn_info, 'epanet_working_copy')
    pgversioning.checkout(['epanet_trunk_rev_head.junctions','epanet_trunk_rev_head.pipes'])

    pcur = versioning.Db(psycopg2.connect(pg_conn_info))

    pcur.execute("UPDATE epanet_working_copy.pipes_view SET length = 4 WHERE id = 1")
    prtTab(pcur, 'epanet_working_copy.pipes_diff')

    prtHid( pcur, 'epanet_working_copy.pipes_view')
    pcur.execute("SElECT COUNT(id) FROM epanet_working_copy.pipes_view")
    assert(1 == pcur.fetchone()[0])

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 versioning_base_test.py host pguser")
    else:
        test(*sys.argv[1:])
