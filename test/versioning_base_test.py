#!/usr/bin/env python3
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB.versioning import diff_rev_view_str

from versioningDB import versioning 
from sqlite3 import dbapi2
import psycopg2
import os
import tempfile

def test(host, pguser):
    pg_conn_info = "dbname=epanet_test_db host=" + host + " user=" + pguser
    tmp_dir = tempfile.gettempdir()
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    sqlite_test_filename1 = os.path.join(tmp_dir, "versioning_base_test1.sqlite")
    sqlite_test_filename2 = os.path.join(tmp_dir, "versioning_base_test2.sqlite")
    sqlite_test_filename3 = os.path.join(tmp_dir, "versioning_base_test3.sqlite")
    sqlite_test_filename4 = os.path.join(tmp_dir, "versioning_base_test4.sqlite")
    sqlite_test_filename5 = os.path.join(tmp_dir, "versioning_base_test5.sqlite")
    if os.path.isfile(sqlite_test_filename1): os.remove(sqlite_test_filename1)
    if os.path.isfile(sqlite_test_filename2): os.remove(sqlite_test_filename2)
    if os.path.isfile(sqlite_test_filename3): os.remove(sqlite_test_filename3)
    if os.path.isfile(sqlite_test_filename4): os.remove(sqlite_test_filename4)
    if os.path.isfile(sqlite_test_filename5): os.remove(sqlite_test_filename5)

    # create the test database

    os.system("dropdb --if-exists -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("createdb -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")
    
    versioning.historize("dbname=epanet_test_db host={} user={}".format(host,pguser), "epanet")

    spversioning1 = versioning.spatialite(sqlite_test_filename1, pg_conn_info)
    spversioning2 = versioning.spatialite(sqlite_test_filename2, pg_conn_info)
    spversioning3 = versioning.spatialite(sqlite_test_filename3, pg_conn_info)
    spversioning4 = versioning.spatialite(sqlite_test_filename4, pg_conn_info)
    spversioning5 = versioning.spatialite(sqlite_test_filename5, pg_conn_info)
    # chechout two tables

    try:
        spversioning1.checkout(["epanet_trunk_rev_head.junctions","epanet.pipes"])
        assert(False and "checkout from schema withouti suffix _branch_rev_head should not be successfull")
    except RuntimeError:
        pass

    assert( not os.path.isfile(sqlite_test_filename1) and "sqlite file must not exist at this point" )
    spversioning1.checkout(["epanet_trunk_rev_head.junctions","epanet_trunk_rev_head.pipes"])
    assert( os.path.isfile(sqlite_test_filename1) and "sqlite file must exist at this point" )

    try:
        spversioning1.checkout(["epanet_trunk_rev_head.junctions","epanet_trunk_rev_head.pipes"])
        assert(False and "trying to checkout on an existing file must fail")
    except RuntimeError:
        pass

    # edit one table and commit changes; rev = 2

    scon = dbapi2.connect(sqlite_test_filename1)
    scon.enable_load_extension(True)
    scon.execute("SELECT load_extension('mod_spatialite')")
    scur = scon.cursor()
    scur.execute("UPDATE junctions_view SET elevation = '8' WHERE id = '2'")
    scon.commit()
    scur.execute("SELECT COUNT(*) FROM junctions")
    assert( scur.fetchone()[0] == 3 )
    scon.close()
    spversioning1.commit('first edit commit')
    pcon = psycopg2.connect(pg_conn_info)
    pcur = pcon.cursor()
    pcur.execute("SELECT COUNT(*) FROM epanet.junctions")
    assert( pcur.fetchone()[0] == 3 )
    pcur.execute("SELECT COUNT(*) FROM epanet.revisions")
    assert( pcur.fetchone()[0] == 2 )

    # add revision : edit one table and commit changes; rev = 3

    spversioning2.checkout(["epanet_trunk_rev_head.junctions", "epanet_trunk_rev_head.pipes"])

    scon = dbapi2.connect(sqlite_test_filename2)
    scon.enable_load_extension(True)
    scon.execute("SELECT load_extension('mod_spatialite')")
    scur = scon.cursor()
    scur.execute("UPDATE junctions_view SET elevation = '22' WHERE id = '2'")
    scon.commit()
    #scur.execute("SELECT COUNT(*) FROM junctions")
    #assert( scur.fetchone()[0] == 3 )
    scon.close()
    spversioning2.commit('second edit commit')

    # add revision : insert one junction and commit changes; rev = 4

    spversioning3.checkout(["epanet_trunk_rev_head.junctions"])

    scon = dbapi2.connect(sqlite_test_filename3)
    scon.enable_load_extension(True)
    scon.execute("SELECT load_extension('mod_spatialite')")
    scur = scon.cursor()
    scur.execute("INSERT INTO junctions_view(id, elevation, geom) VALUES ('10','100',GeomFromText('POINT(2 0)',2154))")
    scon.commit()
    #scur.execute("SELECT COUNT(*) FROM junctions")
    #assert( scur.fetchone()[0] == 3 )
    scon.close()
    spversioning3.commit('insert commit')

    # add revision : delete one junction and commit changes; rev = 5

    spversioning4.checkout(["epanet_trunk_rev_head.junctions", "epanet_trunk_rev_head.pipes"])

    scon = dbapi2.connect(sqlite_test_filename4)
    scur = scon.cursor()

    # remove pipes so wen can delete referenced junctions
    scur.execute("DELETE FROM pipes_view")
    scon.commit()
    scur.execute("SELECT COUNT(*) FROM pipes_view")
    assert(scur.fetchone()[0]==0)
    
    scur.execute("DELETE FROM junctions_view  WHERE id = 1")
    scon.commit()
    #scur.execute("SELECT COUNT(*) FROM junctions")
    #assert( scur.fetchone()[0] == 3 )
    scon.close()
    spversioning4.commit('delete id=1 commit')

    select_str = diff_rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1,2)
    pcur.execute(select_str)
    res = pcur.fetchall()
    assert(res[0][0] == 'u')
    #print("fetchall 1 vs 2 = " + str(res))
    #fetchall 1 vs 2 = [
    #('u', 3, '1', 8.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 2, 2, 2, 4)]

    select_str = diff_rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1,3)
    pcur.execute(select_str)

    res = pcur.fetchall()
    assert(res[0][0] == 'i')
    assert(res[1][0] == 'u')
    #print("fetchall 1 vs 3 = " + str(res))
    #fetchall 1 vs 3 = [
    #('u', 4, '1', 22.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 3, None, 3, None),
    #('i', 3, '1', 8.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 2, 2, 2, 4)]

    select_str = diff_rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1,4)
    pcur.execute(select_str)
    res = pcur.fetchall()
    assert(res[0][0] == 'i')
    assert(res[1][0] == 'i')
    assert(res[2][0] == 'u')
    assert(res[3][0] == 'a') # object is in intermediate state; will be deleted in rev 5
    #print("fetchall 1 vs 4 = " + str(res))
    #fetchall 1 vs 4 = [
    #('u', 4, '1', 22.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 3, None, 3, None),
    #('i', 3, '1', 8.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 2, 2, 2, 4),
    #('a', 5, '10', 100.0, None, None, '01010000206A08000000000000000000400000000000000000', 4, None, None, None),
    #('i', 1, '0', 0.0, None, None, '01010000206A080000000000000000F03F0000000000000000', 1, 4, None, None)]

    select_str = diff_rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1,5)
    pcur.execute(select_str)
    res = pcur.fetchall()
    assert(res[0][0] == 'd')
    assert(res[1][0] == 'i')
    assert(res[2][0] == 'u')
    assert(res[3][0] == 'a')
    #print("fetchall 1 vs 5 = " + str(res))
    #fetchall 1 vs 5 = [
    #('u', 4, '1', 22.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 3, None, 3, None),
    #('i', 3, '1', 8.0, None, None, '01010000206A0800000000000000000000000000000000F03F', 2, 2, 2, 4),
    #('a', 5, '10', 100.0, None, None, '01010000206A08000000000000000000400000000000000000', 4, None, None, None),
    #('d', 1, '0', 0.0, None, None, '01010000206A080000000000000000F03F0000000000000000', 1, 4, None, None)]

    # add revision : edit one table then delete and commit changes; rev = 6

    spversioning5.checkout(["epanet_trunk_rev_head.junctions", "epanet_trunk_rev_head.pipes"])

    scon = dbapi2.connect(sqlite_test_filename5)
    scur = scon.cursor()
    scon.enable_load_extension(True)
    scon.execute("SELECT load_extension('mod_spatialite')")
    scur.execute("UPDATE junctions_view SET elevation = '22' WHERE id = '1'")
    scur.execute("DELETE FROM junctions_view WHERE id = '1'")
    scon.commit()
    #scur.execute("SELECT COUNT(*) FROM junctions")
    #assert( scur.fetchone()[0] == 3 )
    scon.close()
    spversioning5.commit('update and delete commit')


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 versioning_base_test.py host pguser")
    else:
        test(*sys.argv[1:])
