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
    tmp_dir = tempfile.gettempdir()
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    sqlite_test_filename = tmp_dir+"/abbreviation_test.sqlite"
    if os.path.isfile(sqlite_test_filename):
        os.remove(sqlite_test_filename)

    spversioning = versioning.spatialite(sqlite_test_filename, pg_conn_info)
    # create the test database
    os.system("dropdb --if-exists -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("createdb -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -f "+test_data_dir+"/epanet_test_db_unversioned.sql")
    

    pcon = psycopg2.connect(pg_conn_info)
    pcur = pcon.cursor()
    for i in range(10):
        pcur.execute("""
            INSERT INTO epanet.junctions
                (id, elevation, geom)
                VALUES
                ('{id}', {elev}, ST_GeometryFromText('POINT({x} {y})',2154));
            """.format(
                id=str(i+2)+'_this_is_a_very_long_name_that should_be_trunctated_if_buggy',
                elev=float(i),
                x=float(i+1),
                y=float(i+1)
                ))
    pcon.commit()
    versioning.historize(pg_conn_info, 'epanet')

    spversioning.checkout(["epanet_trunk_rev_head.junctions","epanet_trunk_rev_head.pipes"])
    assert( os.path.isfile(sqlite_test_filename) and "sqlite file must exist at this point" )

    scon = dbapi2.connect(sqlite_test_filename)
    scur = scon.cursor()
    scur.execute("SELECT * from junctions")
    for rec in scur:
        if rec[0] > 2:
            assert rec[1].find('_this_is_a_very_long_name_that should_be_trunctated_if_buggy') != -1

    scur.execute("update junctions_view set id='this_is_another_edited_very_long_name_that should_be_trunctated_if_buggy' where ogc_fid > 8")

    scur.execute("insert into junctions_view(id, elevation, geometry) select 'newly inserted with long name', elevation, geometry from junctions_view where ogc_fid=4")
    scon.commit()

    spversioning.commit('a commit msg')

    pcur.execute("select jid, id from epanet_trunk_rev_head.junctions")
    for row in pcur:
        print(row)
        if row[0] > 8:
            assert row[1].find('this_is_another_edited_very_long_name_that should_be_trunctated_if_buggy') != -1\
                or row[1].find('newly inserted with long name') != -1

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python2 versioning_base_test.py host pguser")
    else:
        test(*sys.argv[1:])

