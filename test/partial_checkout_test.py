#!/usr/bin/env python2
import sys
sys.path.insert(0, '..')

from versioningDB import versioning

from pyspatialite import dbapi2
import psycopg2
import os
import tempfile


def test(host, pguser):
    pg_conn_info = "dbname=epanet_test_db host=" + host + " user=" + pguser
    
    tmp_dir = tempfile.gettempdir()
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    sqlite_test_filename = os.path.join(tmp_dir, "partial_checkout_test.sqlite")
    if os.path.isfile(sqlite_test_filename):
        os.remove(sqlite_test_filename)
        
    spversioning = versioning.versioningDb([sqlite_test_filename, pg_conn_info], 'spatialite')

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
                id=i+2,
                elev=float(i),
                x=float(i+1),
                y=float(i+1)
                ))
    pcon.commit()
    pcon.close()

    versioning.historize(pg_conn_info, 'epanet')

    # spatialite working copy
    spversioning.checkout(["epanet_trunk_rev_head.junctions","epanet_trunk_rev_head.pipes"], [[1, 2, 3], []])
    assert( os.path.isfile(sqlite_test_filename) and "sqlite file must exist at this point" )

    scon = dbapi2.connect(sqlite_test_filename)
    scur = scon.cursor()
    scur.execute("SELECT * from junctions")
    assert len(scur.fetchall()) ==  3

    # postgres working copy
    pgversioning = versioning.versioningDb([pg_conn_info, 'my_working_copy'], 'postgres')
    pgversioning.checkout(["epanet_trunk_rev_head.junctions","epanet_trunk_rev_head.pipes"], [[1, 2, 3], []])

    pcon = psycopg2.connect(pg_conn_info)
    pcur = pcon.cursor()
    pcur.execute("SELECT * from my_working_copy.junctions_view")
    assert len(pcur.fetchall()) ==  3


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python2 versioning_base_test.py host pguser")
    else:
        test(*sys.argv[1:])