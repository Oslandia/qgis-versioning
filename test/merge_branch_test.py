# -*- coding: utf-8 -*-

#!/usr/bin/env python3
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB import versioning
import psycopg2
import os


def test(host, pguser):
    pg_conn_info = "dbname=epanet_test_db host=" + host + " user=" + pguser
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    # create the test database

    os.system("dropdb --if-exists -h " + host +
              " -U "+pguser+" epanet_test_db")
    os.system("createdb -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("psql -h " + host + " -U "+pguser +
              " epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql -h " + host + " -U "+pguser +
              " epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")
    versioning.historize("dbname=epanet_test_db host={} user={}".format(host,pguser), "epanet")

    # branch
    versioning.add_branch(pg_conn_info, "epanet", "mybranch", "add 'branch")

    # chechout from branch : epanet_brwcs_rev_head
    #tables = ['epanet_trunk_rev_head.junctions','epanet_trunk_rev_head.pipes']
    tables = ['epanet_mybranch_rev_head.junctions',
              'epanet_mybranch_rev_head.pipes']
    pgversioning = versioning.pgServer(pg_conn_info, 'epanet_brwcs_rev_head')
    pgversioning.checkout(tables)

    pcur = versioning.Db(psycopg2.connect(pg_conn_info))

    # insert into epanet_brwcs_rev_head
    pcur.execute("INSERT INTO epanet_brwcs_rev_head.pipes_view(id, start_node, end_node, geom) VALUES ('2','1','2',ST_GeometryFromText('LINESTRING(1 1,0 1)',2154))")
    pcur.execute("INSERT INTO epanet_brwcs_rev_head.pipes_view(id, start_node, end_node, geom) VALUES ('3','1','2',ST_GeometryFromText('LINESTRING(1 -1,0 1)',2154))")
    pcur.execute("DELETE FROM epanet_brwcs_rev_head.pipes_view WHERE id=3")
    pcur.commit()

    pgversioning.commit("commit", "postgres")

    versioning.merge(pg_conn_info, "epanet", "mybranch")

    pcur.execute("SELECT max(rev) FROM epanet.revisions")
    assert(pcur.fetchone()[0] == 4)

    pcur.execute(
        "SELECT rev, commit_msg, branch FROM epanet.revisions WHERE rev=4")
    assert(pcur.fetchall() == [
           (4, 'Merge branch mybranch into trunk', 'trunk')])

    pcur.execute(
        "SELECT versioning_id, trunk_rev_begin, trunk_rev_end, mybranch_rev_begin,mybranch_rev_end FROM epanet.pipes")
    assert(pcur.fetchall() == [(1, 1, None, 2, None), (2, 3, None, 3, None)])

    pcur.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 merge_branch_test.py host pguser")
    else:
        test(*sys.argv[1:])
