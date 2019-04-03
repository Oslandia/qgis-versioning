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


class ConstraintTest:

    def __init__(self, host, pguser, schema, additional_sql=None):

        self.schema = schema
        self.cur = None
        self.con = None
        self.versioning = None

        self.pg_conn_info = "dbname=epanet_test_db host={} user={}".format(
            host, pguser)

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
            pcon = psycopg2.connect(self.pg_conn_info)
            pcur = pcon.cursor()
            pcur.execute(additional_sql)
            pcon.commit()

        versioning.historize(
            "dbname=epanet_test_db host={} user={}".format(host, pguser),
            "epanet")

    def test_insert(self):

        # insert valid
        res = self.cur.execute(
            "insert into {}.pipes_view (id, start_node, end_node) "
            "values (2,1,2);".format(self.schema))

        self.con.commit()
        self.cur.execute("SELECT COUNT(*) FROM {}.pipes_view".format(
            self.schema))
        assert(self.cur.fetchone()[0] == 2)

        # self.cur.execute("SELECT COUNT(*) FROM {}.pipes".format(b_schema))
        # assert(self.cur.fetchone()[0] == 2)

        # insert fail unique constraint
        try:
            res = self.cur.execute(
                "insert into {}.pipes_view (id, start_node, end_node) "
                "values (1,1,2);".format(self.schema))
            assert(False and "Insert must fail unique constraint")
        except (IntegrityError, psycopg2.InternalError) as e:
            self.con.rollback()
        else:
            self.con.commit()

        # insert fail foreign key constraint
        try:
            res = self.cur.execute(
                "insert into {}.pipes_view (id, start_node, end_node) "
                "values (3,1,3);".format(self.schema))
            assert(False and "Insert must fail foreign key constraint")
        except (IntegrityError, psycopg2.InternalError) as e:
            self.con.rollback()
        else:
            self.con.commit()

    def test_update_referencing(self):

        # insert one more pipe for testing
        res = self.cur.execute("insert into {}.pipes_view "
                               "(id, start_node, end_node) "
                               "values (2,1,2);".format(self.schema))
        self.con.commit()
        self.cur.execute("SELECT COUNT(*) FROM {}.pipes_view".format(
            self.schema))
        assert(self.cur.fetchone()[0] == 2)

        # update nothing to do with self.constraint
        res = self.cur.execute(
            "UPDATE {}.pipes_view SET diameter = '10' WHERE id = 1".format(
                self.schema))
        self.con.commit()
        self.cur.execute(
            "SELECT diameter FROM {}.pipes_view WHERE id = 1".format(
                self.schema))
        assert(self.cur.fetchone()[0] == 10)

        # update valid unique self.constraint
        res = self.cur.execute(
            "UPDATE {}.pipes_view SET id = 3 WHERE id = 1".format(self.schema))
        self.con.commit()
        self.cur.execute("SELECT * FROM {}.pipes_view WHERE id = 3".format(
            self.schema))
        assert(len(self.cur.fetchall()) == 1)

        # update valid foreign key self.constraint
        res = self.cur.execute(
            "UPDATE {}.pipes_view SET start_node = 2 WHERE id = 2".format(
                self.schema))
        self.con.commit()
        self.cur.execute(
            "SELECT start_node FROM {}.pipes_view WHERE id = 2".format(
                self.schema))
        assert(self.cur.fetchone()[0] == 2)

        # update fail unique self.constraint
        try:
            res = self.cur.execute(
                "UPDATE {}.pipes_view SET ID = 2 WHERE id = 3".format(
                    self.schema))
            assert(False and "Insert must fail unique self.constraint")
        except (IntegrityError, psycopg2.InternalError) as e:
            self.con.rollback()
        else:
            self.con.commit()

        # update fail foreign key self.constraint
        try:
            res = self.cur.execute("UPDATE {}.pipes_view SET start_node = 3 "
                                   "WHERE id = 2".format(self.schema))
            assert(False and "Insert must fail foreign key self.constraint")
        except (IntegrityError, psycopg2.InternalError) as e:
            self.con.rollback()
        else:
            self.con.commit()

    def test_delete_restrict(self):

        # delete is restrict, must fail
        try:
            res = self.cur.execute(
                "DELETE FROM {}.junctions_view WHERE id = 1".format(
                    self.schema))
            assert(False and "Delete must fail because of referenced key")
        except (IntegrityError, psycopg2.InternalError) as e:
            self.con.rollback()
        else:
            self.con.commit()

    def test_delete_cascade(self):

        res = self.cur.execute(
            "DELETE FROM {}.junctions_view WHERE id = 1".format(self.schema))
        self.con.commit()

        self.cur.execute("SELECT * FROM {}.junctions_view".format(self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute("SELECT * FROM {}.pipes_view".format(self.schema))
        assert(len(self.cur.fetchall()) == 0)

    def test_update_cascade(self):

        res = self.cur.execute(
            "UPDATE {}.junctions_view SET id = 3 WHERE id = 1".format(
                self.schema))
        self.con.commit()
        self.cur.execute(
            "SELECT count(*) FROM {}.junctions_view WHERE id = 3".format(
                self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute(
            "SELECT * FROM {}.pipes_view WHERE start_node = 3".format(
                self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute(
            "SELECT * FROM {}.pipes_view WHERE start_node = 1".format(
                self.schema))
        assert(len(self.cur.fetchall()) == 0)

    def test_delete_setnull(self):

        res = self.cur.execute(
            "DELETE FROM {}.junctions_view WHERE id = 1".format(self.schema))
        self.con.commit()

        self.cur.execute(
            "SELECT * FROM {}.junctions_view".format(self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute(
            "SELECT start_node, end_node FROM {}.pipes_view".format(self.schema))
        pipes = self.cur.fetchall()
        assert(len(pipes) == 1)
        assert(pipes[0][0] is None)
        assert(pipes[0][1] == 2)

    def test_update_setnull(self):

        res = self.cur.execute(
            "UPDATE {}.junctions_view SET id = 3 WHERE id = 1".format(self.schema))
        self.con.commit()
        self.cur.execute(
            "SELECT count(*) FROM {}.junctions_view WHERE id = 3".format(self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute(
            "SELECT start_node, end_node FROM {}.pipes_view".format(self.schema))
        pipes = self.cur.fetchall()
        assert(len(pipes) == 1)
        assert(pipes[0][0] is None)
        assert(pipes[0][1] == 2)

    def test_delete_setdefault(self):

        res = self.cur.execute(
            "DELETE FROM {}.junctions_view WHERE id = 1".format(self.schema))
        self.con.commit()

        self.cur.execute(
            "SELECT * FROM {}.junctions_view".format(self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute(
            "SELECT start_node, end_node FROM {}.pipes_view".format(self.schema))
        pipes = self.cur.fetchall()
        assert(len(pipes) == 1)
        assert(pipes[0][0] == 2)
        assert(pipes[0][1] == 2)

    def test_update_setdefault(self):

        res = self.cur.execute(
            "UPDATE {}.junctions_view SET id = 3 WHERE id = 1".format(self.schema))
        self.con.commit()
        self.cur.execute(
            "SELECT count(*) FROM {}.junctions_view WHERE id = 3".format(self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute(
            "SELECT start_node, end_node FROM {}.pipes_view".format(self.schema))
        pipes = self.cur.fetchall()
        assert(len(pipes) == 1)
        assert(pipes[0][0] == 2)
        assert(pipes[0][1] == 2)


class ConstraintSpatialiteTest(ConstraintTest):

    def __init__(self, host, pguser, additional_sql=None):

        super().__init__(host, pguser, "main", additional_sql)

        if os.path.isfile(sqlite_test_filename):
            os.remove(sqlite_test_filename)

        self.versioning = versioning.spatialite(sqlite_test_filename,
                                                self.pg_conn_info)
        self.versioning.checkout(["epanet_trunk_rev_head.pipes"])

        self.con = dbapi2.connect(sqlite_test_filename)
        self.con.enable_load_extension(True)
        self.con.execute("SELECT load_extension('mod_spatialite')")
        self.cur = self.con.cursor()

    def __del__(self):
        if self.con:
            self.con.close()


class ConstraintPgServerTest(ConstraintTest):

    def __init__(self, host, pguser, additional_sql=None):

        wc_schema = "epanet_workingcopy"
        super().__init__(host, pguser, wc_schema, additional_sql)

        self.versioning = versioning.pgServer(self.pg_conn_info,
                                              wc_schema)
        self.versioning.checkout(["epanet_trunk_rev_head.pipes"])

        self.con = psycopg2.connect(self.pg_conn_info)
        self.cur = self.con.cursor()


def test(host, pguser):

    # loop on the 3 ways of checkout (sqlite, pgserver, pglocal)
    for test_class in [ConstraintSpatialiteTest, ConstraintPgServerTest]:

        test = test_class(host, pguser)
        test.test_insert()
        del test

        test = test_class(host, pguser)
        test.test_update_referencing()
        del test

        test = test_class(host, pguser)
        test.test_delete_restrict()
        del test

        test = test_class(host, pguser, sql_modify_fkey.format(
            ftype="ON DELETE CASCADE"))
        test.test_delete_cascade()
        del test

        test = test_class(host, pguser, sql_modify_fkey.format(
            ftype="ON update CASCADE"))
        test.test_update_cascade()
        del test

        test = test_class(host, pguser,
                          sql_modify_fkey.format(ftype="ON DELETE SET NULL"))
        test.test_delete_setnull()
        del test

        test = test_class(host, pguser,
                          sql_modify_fkey.format(ftype="ON UPDATE SET NULL"))
        test.test_update_setnull()
        del test

        sql = sql_modify_fkey.format(ftype="ON DELETE SET DEFAULT")
        sql += "ALTER TABLE epanet.pipes ALTER COLUMN start_node SET DEFAULT 2;"
        test = test_class(host, pguser, sql)
        test.test_delete_setdefault()
        del test

        sql = sql_modify_fkey.format(ftype="ON UPDATE SET DEFAULT")
        sql += "ALTER TABLE epanet.pipes ALTER COLUMN start_node SET DEFAULT 2;"
        test = test_class(host, pguser, sql)
        test.test_update_setdefault()
        del test


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 constraints_test.py host pguser")
    else:
        test(*sys.argv[1:])
