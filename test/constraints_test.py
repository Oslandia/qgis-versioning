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

pgserver_workingcopy_schema = "epanet_workingcopy"


def load_database(host, pguser, additional_sql=None):

    pg_conn_info = "dbname=epanet_test_db host=" + host + " user=" + pguser
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    if os.path.isfile(sqlite_test_filename):
        os.remove(sqlite_test_filename)

    # create the test database
    os.system("dropdb --if-exists -h " + host + " -U "+pguser
              + " epanet_test_db")
    os.system("createdb -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -f "
              + test_data_dir + "/epanet_test_db.sql")

    if additional_sql:
        pcon = psycopg2.connect(pg_conn_info)
        pcur = pcon.cursor()
        pcur.execute(additional_sql)
        pcon.commit()

    versioning.historize("dbname=epanet_test_db host={} user={}".format(
        host, pguser), "epanet")

    return pg_conn_info


def load_spatialite_database(host, pguser, additional_sql=None):

    pg_conn_info = load_database(host, pguser, additional_sql)

    if os.path.isfile(sqlite_test_filename):
        os.remove(sqlite_test_filename)

    spversioning = versioning.spatialite(sqlite_test_filename, pg_conn_info)
    spversioning.checkout(["epanet_trunk_rev_head.pipes"])

    scon = dbapi2.connect(sqlite_test_filename)
    scon.enable_load_extension(True)
    scon.execute("SELECT load_extension('mod_spatialite')")

    return scon


def load_pgserver_database(host, pguser, additional_sql=None):

    pg_conn_info = load_database(host, pguser, additional_sql)

    pgs_versioning = versioning.pgServer(pg_conn_info,
                                         pgserver_workingcopy_schema)
    pgs_versioning.checkout(["epanet_trunk_rev_head.pipes"])

    pcon = psycopg2.connect("dbname=epanet_test_db host={} user={}".format(
        host, pguser))

    return pcon


def test_insert(con, cur, schema):

    # insert valid
    res = cur.execute(
        "insert into {}.pipes_view (id, start_node, end_node) "
        "values (2,1,2);".format(schema))

    con.commit()
    cur.execute("SELECT COUNT(*) FROM {}.pipes_view".format(schema))
    assert(cur.fetchone()[0] == 2)

    # insert fail unique constraint
    try:
        res = cur.execute(
            "insert into {}.pipes_view (id, start_node, end_node) "
            "values (1,1,2);".format(schema))
        assert(False and "Insert must fail unique constraint")
    except (IntegrityError, psycopg2.InternalError) as e:
        con.rollback()
    else:
        con.commit()

    # insert fail foreign key constraint
    try:
        res = cur.execute(
            "insert into {}.pipes_view (id, start_node, end_node) "
            "values (3,1,3);".format(schema))
        assert(False and "Insert must fail foreign key constraint")
    except (IntegrityError, psycopg2.InternalError) as e:
        con.rollback()
    else:
        con.commit()


def test_update_referencing(con, cur, schema):

    # insert one more pipe for testing
    res = cur.execute("insert into {}.pipes_view (id, start_node, end_node) "
                       "values (2,1,2);".format(schema))
    con.commit()
    cur.execute("SELECT COUNT(*) FROM {}.pipes_view".format(schema))
    assert(cur.fetchone()[0] == 2)

    # update nothing to do with constraint
    res = cur.execute(
        "UPDATE {}.pipes_view SET diameter = '10' WHERE id = 1".format(schema))
    con.commit()
    cur.execute(
        "SELECT diameter FROM {}.pipes_view WHERE id = 1".format(schema))
    assert(cur.fetchone()[0] == 10)

    # update valid unique constraint
    res = cur.execute(
        "UPDATE {}.pipes_view SET id = 3 WHERE id = 1".format(schema))
    con.commit()
    cur.execute("SELECT * FROM {}.pipes_view WHERE id = 3".format(schema))
    assert(len(cur.fetchall()) == 1)

    # update valid foreign key constraint
    res = cur.execute(
        "UPDATE {}.pipes_view SET start_node = 2 WHERE id = 2".format(schema))
    con.commit()
    cur.execute(
        "SELECT start_node FROM {}.pipes_view WHERE id = 2".format(schema))
    assert(cur.fetchone()[0] == 2)

    # update fail unique constraint
    try:
        res = cur.execute(
            "UPDATE {}.pipes_view SET ID = 2 WHERE id = 3".format(schema))
        assert(False and "Insert must fail unique constraint")
    except (IntegrityError, psycopg2.InternalError) as e:
        con.rollback()
    else:
        con.commit()

    # update fail foreign key constraint
    try:
        res = cur.execute("UPDATE {}.pipes_view SET start_node = 3 "
                           "WHERE id = 2".format(schema))
        assert(False and "Insert must fail foreign key constraint")
    except (IntegrityError, psycopg2.InternalError) as e:
        con.rollback()
    else:
        con.commit()


def test_delete_restrict(con, cur, schema):

    # delete is restrict, must fail
    try:
        res = cur.execute(
            "DELETE FROM {}.junctions_view WHERE id = 1".format(schema))
        assert(False and "Delete must fail because of referenced key")
    except (IntegrityError, psycopg2.InternalError) as e:
        con.rollback()
    else:
        con.commit()


def test_delete_cascade(con, cur, schema):

    res = cur.execute(
        "DELETE FROM {}.junctions_view WHERE id = 1".format(schema))
    con.commit()

    cur.execute("SELECT * FROM {}.junctions_view".format(schema))
    assert(len(cur.fetchall()) == 1)

    cur.execute("SELECT * FROM {}.pipes_view".format(schema))
    assert(len(cur.fetchall()) == 0)


def test_update_cascade(con, cur, schema):

    res = cur.execute(
        "UPDATE {}.junctions_view SET id = 3 WHERE id = 1".format(schema))
    con.commit()
    cur.execute(
        "SELECT count(*) FROM {}.junctions_view WHERE id = 3".format(schema))
    assert(len(cur.fetchall()) == 1)

    cur.execute(
        "SELECT * FROM {}.pipes_view WHERE start_node = 3".format(schema))
    assert(len(cur.fetchall()) == 1)

    cur.execute(
        "SELECT * FROM {}.pipes_view WHERE start_node = 1".format(schema))
    assert(len(cur.fetchall()) == 0)


def test_delete_setnull(con, cur, schema):

    res = cur.execute(
        "DELETE FROM {}.junctions_view WHERE id = 1".format(schema))
    con.commit()

    cur.execute(
        "SELECT * FROM {}.junctions_view".format(schema))
    assert(len(cur.fetchall()) == 1)

    cur.execute(
        "SELECT start_node, end_node FROM {}.pipes_view".format(schema))
    pipes = cur.fetchall()
    assert(len(pipes) == 1)
    assert(pipes[0][0] is None)
    assert(pipes[0][1] == 2)


def test_update_setnull(con, cur, schema):

    res = cur.execute(
        "UPDATE {}.junctions_view SET id = 3 WHERE id = 1".format(schema))
    con.commit()
    cur.execute(
        "SELECT count(*) FROM {}.junctions_view WHERE id = 3".format(schema))
    assert(len(cur.fetchall()) == 1)

    cur.execute(
        "SELECT start_node, end_node FROM {}.pipes_view".format(schema))
    pipes = cur.fetchall()
    assert(len(pipes) == 1)
    assert(pipes[0][0] is None)
    assert(pipes[0][1] == 2)


def test_delete_setdefault(con, cur, schema):

    res = cur.execute(
        "DELETE FROM {}.junctions_view WHERE id = 1".format(schema))
    con.commit()

    cur.execute(
        "SELECT * FROM {}.junctions_view".format(schema))
    assert(len(cur.fetchall()) == 1)

    cur.execute(
        "SELECT start_node, end_node FROM {}.pipes_view".format(schema))
    pipes = cur.fetchall()
    assert(len(pipes) == 1)
    assert(pipes[0][0] == 2)
    assert(pipes[0][1] == 2)


def test_update_setdefault(con, cur, schema):

    res = cur.execute(
        "UPDATE {}.junctions_view SET id = 3 WHERE id = 1".format(schema))
    con.commit()
    cur.execute(
        "SELECT count(*) FROM {}.junctions_view WHERE id = 3".format(schema))
    assert(len(cur.fetchall()) == 1)

    cur.execute(
        "SELECT start_node, end_node FROM {}.pipes_view".format(schema))
    pipes = cur.fetchall()
    assert(len(pipes) == 1)
    assert(pipes[0][0] == 2)
    assert(pipes[0][1] == 2)


def test(host, pguser):

    # loop on the 3 ways of checkout (sqlite, pgserver, pglocal)
    for func, schema in [
            (load_spatialite_database, "main"),
            (load_pgserver_database, pgserver_workingcopy_schema)]:

        con = func(host, pguser)
        test_insert(con, con.cursor(), schema)
        con.close()

        con = func(host, pguser)
        test_update_referencing(con, con.cursor(), schema)
        con.close()

        con = func(host, pguser)
        test_delete_restrict(con, con.cursor(), schema)
        con.close()

        con = func(host, pguser, sql_modify_fkey.format(
            ftype="ON DELETE CASCADE"))
        test_delete_cascade(con, con.cursor(), schema)
        con.close()

        con = func(host, pguser, sql_modify_fkey.format(
            ftype="ON update CASCADE"))
        test_update_cascade(con, con.cursor(), schema)
        con.close()

        con = func(host, pguser,
                   sql_modify_fkey.format(ftype="ON DELETE SET NULL"))
        test_delete_setnull(con, con.cursor(), schema)
        con.close()

        con = func(host, pguser,
                   sql_modify_fkey.format(ftype="ON UPDATE SET NULL"))
        test_update_setnull(con, con.cursor(), schema)
        con.close()

        sql = sql_modify_fkey.format(ftype="ON DELETE SET DEFAULT")
        sql += "ALTER TABLE epanet.pipes ALTER COLUMN start_node SET DEFAULT 2;"
        con = func(host, pguser, sql)
        test_delete_setdefault(con, con.cursor(), schema)
        con.close()

        sql = sql_modify_fkey.format(ftype="ON UPDATE SET DEFAULT")
        sql += "ALTER TABLE epanet.pipes ALTER COLUMN start_node SET DEFAULT 2;"
        con = func(host, pguser, sql)
        test_update_setdefault(con, con.cursor(), schema)
        con.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 constraints_test.py host pguser")
    else:
        test(*sys.argv[1:])
