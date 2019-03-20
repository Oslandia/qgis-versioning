#!/usr/bin/env python3
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB import versioning 
from sqlite3 import dbapi2, IntegrityError
import psycopg2
import os
import tempfile


def test(host, pguser):
    pg_conn_info = "dbname=epanet_test_db host=" + host + " user=" + pguser
    tmp_dir = tempfile.gettempdir()
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    sqlite_test_filename = os.path.join(tmp_dir, "constraints_test.sqlite")
    if os.path.isfile(sqlite_test_filename):
        os.remove(sqlite_test_filename)

    # create the test database
    os.system("dropdb --if-exists -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("createdb -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")
    versioning.historize("dbname=epanet_test_db host={} user={}".format(host,pguser), "epanet")

    spversioning = versioning.spatialite(sqlite_test_filename, pg_conn_info)

    spversioning.checkout(["epanet_trunk_rev_head.pipes"])

    scon = dbapi2.connect(sqlite_test_filename)
    scon.enable_load_extension(True)
    scon.execute("SELECT load_extension('mod_spatialite')")
    scur = scon.cursor()

    # insert valid
    res = scur.execute("insert into pipes_view (id, start_node, end_node) "
                       "values (2,1,2);")
    scon.commit()
    scur.execute("SELECT COUNT(*) FROM pipes_view")
    assert(scur.fetchone()[0] == 2)

    # insert fail unique constraint
    try:
        res = scur.execute("insert into pipes_view (id, start_node, end_node) "
                           "values (1,1,2);")
        scon.commit()
        assert(False and "Insert must fail unique constraint")
    except IntegrityError:
        pass

    # insert fail foreign key constraint
    try:
        res = scur.execute("insert into pipes_view (id, start_node, end_node) "
                           "values (3,1,3);")
        scon.commit()
        assert(False and "Insert must fail foreign key constraint")
    except IntegrityError:
        pass

    # update nothing to do with constraint
    res = scur.execute("UPDATE pipes_view SET diameter = '10' WHERE id = 1")
    scon.commit()
    scur.execute("SELECT diameter FROM pipes_view WHERE id = 1")
    assert(scur.fetchone()[0] == 10)

    # update valid unique constraint
    res = scur.execute("UPDATE pipes_view SET id = 3 WHERE id = 1")
    scon.commit()
    scur.execute("SELECT * FROM pipes_view WHERE id = 3")
    assert(len(scur.fetchall()) == 1)

    # update valid foreign key constraint
    res = scur.execute("UPDATE pipes_view SET start_node = 2 WHERE id = 2")
    scon.commit()
    scur.execute("SELECT start_node FROM pipes_view WHERE id = 2")
    assert(scur.fetchone()[0] == 2)

    # update fail unique constraint
    try:
        res = scur.execute("UPDATE pipes_view SET ID = 2 WHERE id = 3")
        scon.commit()
        assert(False and "Insert must fail unique constraint")
    except IntegrityError:
        pass

    # update fail foreign key constraint
    try:
        res = scur.execute("UPDATE pipes_view SET start_node = 3 "
                           "WHERE id = 2")
        scon.commit()
        assert(False and "Insert must fail foreign key constraint")
    except IntegrityError:
        pass


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 constraints_test.py host pguser")
    else:
        test(*sys.argv[1:])
