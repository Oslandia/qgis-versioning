#!/usr/bin/env python2
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB.sp_versioning import spVersioning
from versioningDB.versioning import diff_rev_view_str
from pyspatialite import dbapi2
import psycopg2
import os
import tempfile

PGUSER = 'postgres'
HOST = '127.0.0.1'
pg_conn_info = "dbname=epanet_test_db host="+HOST+" user="+PGUSER

def test():
    tmp_dir = tempfile.gettempdir()
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    sqlite_test_filename1 = tmp_dir+"/versioning_base_test1.sqlite"
    sqlite_test_filename2 = tmp_dir+"/versioning_base_test2.sqlite"
    sqlite_test_filename3 = tmp_dir+"/versioning_base_test3.sqlite"
    sqlite_test_filename4 = tmp_dir+"/versioning_base_test4.sqlite"
    sqlite_test_filename5 = tmp_dir+"/versioning_base_test5.sqlite"
    if os.path.isfile(sqlite_test_filename1): os.remove(sqlite_test_filename1)
    if os.path.isfile(sqlite_test_filename2): os.remove(sqlite_test_filename2)
    if os.path.isfile(sqlite_test_filename3): os.remove(sqlite_test_filename3)
    if os.path.isfile(sqlite_test_filename4): os.remove(sqlite_test_filename4)
    if os.path.isfile(sqlite_test_filename5): os.remove(sqlite_test_filename5)

    # create the test database

    os.system("dropdb --if-exists -h " + HOST + " -U "+PGUSER+" epanet_test_db")
    os.system("createdb -h " + HOST + " -U "+PGUSER+" epanet_test_db")
    os.system("psql -h " + HOST + " -U "+PGUSER+" epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql -h " + HOST + " -U "+PGUSER+" epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")

    versioning = spVersioning()
    # chechout two tables

    try:
        versioning.checkout(pg_conn_info,["epanet_trunk_rev_head.junctions","epanet.pipes"], sqlite_test_filename1)
        assert(False and "checkout from schema withouti suffix _branch_rev_head should not be successfull")
    except RuntimeError:
        pass

    assert( not os.path.isfile(sqlite_test_filename1) and "sqlite file must not exist at this point" )
    versioning.checkout(pg_conn_info,["epanet_trunk_rev_head.junctions","epanet_trunk_rev_head.pipes"], sqlite_test_filename1)
    assert( os.path.isfile(sqlite_test_filename1) and "sqlite file must exist at this point" )

    try:
        versioning.checkout(pg_conn_info,["epanet_trunk_rev_head.junctions","epanet_trunk_rev_head.pipes"], sqlite_test_filename1)
        assert(False and "trying to checkout on an existing file must fail")
    except RuntimeError:
        pass

    # edit one table and commit changes; rev = 2

    scon = dbapi2.connect(sqlite_test_filename1)
    scur = scon.cursor()
    scur.execute("UPDATE junctions_view SET elevation = '8' WHERE id = '1'")
    scon.commit()
    scur.execute("SELECT COUNT(*) FROM junctions")
    assert( scur.fetchone()[0] == 3 )
    scon.close()
    versioning.commit([sqlite_test_filename1, pg_conn_info], 'first edit commit')
    pcon = psycopg2.connect(pg_conn_info)
    pcur = pcon.cursor()
    pcur.execute("SELECT COUNT(*) FROM epanet.junctions")
    assert( pcur.fetchone()[0] == 3 )
    pcur.execute("SELECT COUNT(*) FROM epanet.revisions")
    assert( pcur.fetchone()[0] == 2 )

    # add revision : edit one table and commit changes; rev = 3

    versioning.checkout(pg_conn_info,["epanet_trunk_rev_head.junctions"], sqlite_test_filename2)

    scon = dbapi2.connect(sqlite_test_filename2)
    scur = scon.cursor()
    scur.execute("UPDATE junctions_view SET elevation = '22' WHERE id = '1'")
    scon.commit()
    #scur.execute("SELECT COUNT(*) FROM junctions")
    #assert( scur.fetchone()[0] == 3 )
    scon.close()
    versioning.commit([sqlite_test_filename2, pg_conn_info], 'second edit commit')

    # add revision : insert one junction and commit changes; rev = 4

    versioning.checkout(pg_conn_info,["epanet_trunk_rev_head.junctions"], sqlite_test_filename3)

    scon = dbapi2.connect(sqlite_test_filename3)
    scur = scon.cursor()
    scur.execute("INSERT INTO junctions_view(id, elevation, GEOMETRY) VALUES ('10','100',GeomFromText('POINT(2 0)',2154))")
    scon.commit()
    #scur.execute("SELECT COUNT(*) FROM junctions")
    #assert( scur.fetchone()[0] == 3 )
    scon.close()
    versioning.commit([sqlite_test_filename3, pg_conn_info], 'insert commit')

    # add revision : delete one junction and commit changes; rev = 5

    versioning.checkout(pg_conn_info,["epanet_trunk_rev_head.junctions"], sqlite_test_filename4)

    scon = dbapi2.connect(sqlite_test_filename4)
    scur = scon.cursor()
    scur.execute("DELETE FROM junctions_view  WHERE id = 0")
    scon.commit()
    #scur.execute("SELECT COUNT(*) FROM junctions")
    #assert( scur.fetchone()[0] == 3 )
    scon.close()
    versioning.commit([sqlite_test_filename4, pg_conn_info], 'delete id=0 commit')

    select_str = diff_rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1,2)
    pcur.execute(select_str)
    res = pcur.fetchall()
    assert(res[0][0] == 'u')
    #print "fetchall 1 vs 2 = " + str(res)
    #fetchall 1 vs 2 = [
    #('u', 3, '1', 8.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 2, 2, 2, 4)]

    select_str = diff_rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1,3)
    pcur.execute(select_str)
    res = pcur.fetchall()
    assert(res[0][0] == 'u')
    assert(res[1][0] == 'i')
    #print "fetchall 1 vs 3 = " + str(res)
    #fetchall 1 vs 3 = [
    #('u', 4, '1', 22.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 3, None, 3, None),
    #('i', 3, '1', 8.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 2, 2, 2, 4)]

    select_str = diff_rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1,4)
    pcur.execute(select_str)
    res = pcur.fetchall()
    assert(res[0][0] == 'u')
    assert(res[1][0] == 'i')
    assert(res[2][0] == 'a')
    assert(res[3][0] == 'i') # object is in intermediate state; will be deleted in rev 5
    #print "fetchall 1 vs 4 = " + str(res)
    #fetchall 1 vs 4 = [
    #('u', 4, '1', 22.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 3, None, 3, None),
    #('i', 3, '1', 8.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 2, 2, 2, 4),
    #('a', 5, '10', 100.0, None, None, '01010000206A08000000000000000000400000000000000000', 4, None, None, None),
    #('i', 1, '0', 0.0, None, None, '01010000206A080000000000000000F03F0000000000000000', 1, 4, None, None)]

    select_str = diff_rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1,5)
    pcur.execute(select_str)
    res = pcur.fetchall()
    assert(res[0][0] == 'u')
    assert(res[1][0] == 'i')
    assert(res[2][0] == 'a')
    assert(res[3][0] == 'd')
    #print "fetchall 1 vs 5 = " + str(res)
    #fetchall 1 vs 5 = [
    #('u', 4, '1', 22.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 3, None, 3, None),
    #('i', 3, '1', 8.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 2, 2, 2, 4),
    #('a', 5, '10', 100.0, None, None, '01010000206A08000000000000000000400000000000000000', 4, None, None, None),
    #('d', 1, '0', 0.0, None, None, '01010000206A080000000000000000F03F0000000000000000', 1, 4, None, None)]

    # add revision : edit one table then delete and commit changes; rev = 6

    versioning.checkout(pg_conn_info,["epanet_trunk_rev_head.junctions"], sqlite_test_filename5)

    scon = dbapi2.connect(sqlite_test_filename5)
    scur = scon.cursor()
    scur.execute("UPDATE junctions_view SET elevation = '22' WHERE id = '1'")
    scur.execute("DELETE FROM junctions_view WHERE id = '1'")
    scon.commit()
    #scur.execute("SELECT COUNT(*) FROM junctions")
    #assert( scur.fetchone()[0] == 3 )
    scon.close()
    versioning.commit([sqlite_test_filename5, pg_conn_info], 'update and delete commit')


if __name__ == "__main__":
    test()
