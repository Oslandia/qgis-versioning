#!/usr/bin/env python3
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB import versioning 
from sqlite3 import dbapi2, IntegrityError
import psycopg2
import os
import tempfile

tmp_dir = tempfile.gettempdir()
sqlite_test_filename = os.path.join(tmp_dir, "constraints_test.sqlite")

sql_modify_fkey = """
    ALTER TABLE epanet.pipes
    DROP CONSTRAINT pipes_start_node_fkey;
    ALTER TABLE epanet.pipes
    DROP CONSTRAINT pipes_end_node_fkey;

    ALTER TABLE epanet.pipes
    ADD CONSTRAINT pipes_start_node_fkey
    FOREIGN KEY (start_node)
    REFERENCES epanet.junctions(id)
    {ftype};

    ALTER TABLE epanet.pipes
    ADD CONSTRAINT pipes_end_node_fkey
    FOREIGN KEY (end_node)
    REFERENCES epanet.junctions(id)
    {ftype};
    """

def load_test_database(host, pguser, additional_sql=None):

    pg_conn_info = "dbname=epanet_test_db host=" + host + " user=" + pguser
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    if os.path.isfile(sqlite_test_filename):
        os.remove(sqlite_test_filename)

    # create the test database
    os.system("dropdb --if-exists -h " + host + " -U "+pguser
              + " epanet_test_db")
    os.system("createdb -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("psql -h " + host + " -U "+pguser
              + " epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -f "
              + test_data_dir + "/epanet_test_db.sql")

    if additional_sql:
        pcon = psycopg2.connect(pg_conn_info)
        pcur = pcon.cursor()
        pcur.execute(additional_sql)
        pcon.commit()

    versioning.historize("dbname=epanet_test_db host={} user={}".format(
        host, pguser), "epanet")

    spversioning = versioning.spatialite(sqlite_test_filename, pg_conn_info)
    spversioning.checkout(["epanet_trunk_rev_head.pipes"])

    scon = dbapi2.connect(sqlite_test_filename)
    scon.enable_load_extension(True)
    scon.execute("SELECT load_extension('mod_spatialite')")

    return (spversioning, scon)


def test_insert(host, pguser):

    spversioning, scon = load_test_database(host, pguser)

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

def test_update_referencing(host, pguser):

    spversioning, scon = load_test_database(host, pguser)
    scur = scon.cursor()

    # insert one more pipe for testing
    res = scur.execute("insert into pipes_view (id, start_node, end_node) "
                       "values (2,1,2);")
    scon.commit()
    scur.execute("SELECT COUNT(*) FROM pipes_view")
    assert(scur.fetchone()[0] == 2)

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


def test_delete_restrict(host, pguser):

    spversioning, scon = load_test_database(host, pguser)
    scur = scon.cursor()

    # delete is restrict, must fail
    try:
        res = scur.execute("DELETE FROM junctions_view WHERE id = 1")
        scon.commit()
        assert(False and "Delete must fail because of referenced key")
    except IntegrityError:
        pass

    scon.commit()
    pass


def test_delete_cascade(host, pguser):

    # set foreign key to on delete cascade
    sql = sql_modify_fkey.format(ftype="ON DELETE CASCADE")

    spversioning, scon = load_test_database(host, pguser, sql)
    scur = scon.cursor()

    res = scur.execute("DELETE FROM junctions_view WHERE id = 1")
    scon.commit()

    scur.execute("SELECT * FROM junctions_view")
    assert(len(scur.fetchall()) == 1)

    scur.execute("SELECT * FROM pipes_view")
    assert(len(scur.fetchall()) == 0)


def test_update_cascade(host, pguser):

    # set foreign key to on delete cascade
    sql = sql_modify_fkey.format(ftype="ON UPDATE CASCADE")

    spversioning, scon = load_test_database(host, pguser, sql)
    scur = scon.cursor()

    res = scur.execute("UPDATE junctions_view SET id = 3 WHERE id = 1")
    scon.commit()
    scur.execute("SELECT count(*) FROM junctions_view WHERE id = 3")
    assert(len(scur.fetchall()) == 1)

    scur.execute("SELECT * FROM pipes_view WHERE start_node = 3")
    assert(len(scur.fetchall()) == 1)

    scur.execute("SELECT * FROM pipes_view WHERE start_node = 1")
    assert(len(scur.fetchall()) == 0)


def test_delete_setnull(host, pguser):

    # set foreign key to on delete cascade
    sql = sql_modify_fkey.format(ftype="ON DELETE SET NULL")

    spversioning, scon = load_test_database(host, pguser, sql)
    scur = scon.cursor()

    res = scur.execute("DELETE FROM junctions_view WHERE id = 1")
    scon.commit()

    scur.execute("SELECT * FROM junctions_view")
    assert(len(scur.fetchall()) == 1)

    scur.execute("SELECT start_node, end_node FROM pipes_view")
    pipes = scur.fetchall()
    assert(len(pipes) == 1)
    assert(pipes[0][0] is None)
    assert(pipes[0][1] == 2)


def test_update_setnull(host, pguser):

    # set foreign key to on delete cascade
    sql = sql_modify_fkey.format(ftype="ON UPDATE SET NULL")

    spversioning, scon = load_test_database(host, pguser, sql)
    scur = scon.cursor()

    res = scur.execute("UPDATE junctions_view SET id = 3 WHERE id = 1")
    scon.commit()
    scur.execute("SELECT count(*) FROM junctions_view WHERE id = 3")
    assert(len(scur.fetchall()) == 1)

    scur.execute("SELECT start_node, end_node FROM pipes_view")
    pipes = scur.fetchall()
    assert(len(pipes) == 1)
    assert(pipes[0][0] is None)
    assert(pipes[0][1] == 2)


def test_delete_setdefault(host, pguser):

    # set foreign key to on delete cascade
    sql = sql_modify_fkey.format(ftype="ON DELETE SET DEFAULT")
    sql += "ALTER TABLE epanet.pipes ALTER COLUMN start_node SET DEFAULT 2;"

    spversioning, scon = load_test_database(host, pguser, sql)
    scur = scon.cursor()

    res = scur.execute("DELETE FROM junctions_view WHERE id = 1")
    scon.commit()

    scur.execute("SELECT * FROM junctions_view")
    assert(len(scur.fetchall()) == 1)

    scur.execute("SELECT start_node, end_node FROM pipes_view")
    pipes = scur.fetchall()
    assert(len(pipes) == 1)
    assert(pipes[0][0] == 2)
    assert(pipes[0][1] == 2)


def test_update_setdefault(host, pguser):

    # set foreign key to on delete cascade
    sql = sql_modify_fkey.format(ftype="ON UPDATE SET DEFAULT")
    sql += "ALTER TABLE epanet.pipes ALTER COLUMN start_node SET DEFAULT 2;"

    spversioning, scon = load_test_database(host, pguser, sql)
    scur = scon.cursor()

    res = scur.execute("UPDATE junctions_view SET id = 3 WHERE id = 1")
    scon.commit()
    scur.execute("SELECT count(*) FROM junctions_view WHERE id = 3")
    assert(len(scur.fetchall()) == 1)

    scur.execute("SELECT start_node, end_node FROM pipes_view")
    pipes = scur.fetchall()
    assert(len(pipes) == 1)
    assert(pipes[0][0] == 2)
    assert(pipes[0][1] == 2)


def test(host, pguser):

    test_insert(host, pguser)
    test_update_referencing(host, pguser)
    test_delete_restrict(host, pguser)
    test_delete_cascade(host, pguser)
    test_update_cascade(host, pguser)
    test_delete_setnull(host, pguser)
    test_update_setnull(host, pguser)
    test_delete_setdefault(host, pguser)
    test_update_setdefault(host, pguser)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 constraints_test.py host pguser")
    else:
        test(*sys.argv[1:])
