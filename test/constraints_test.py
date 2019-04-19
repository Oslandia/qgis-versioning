#!/usr/bin/env python3

from versioningDB import versioning
from sqlite3 import dbapi2, IntegrityError
import psycopg2
import os
import tempfile
import sys

tmp_dir = tempfile.gettempdir()
sqlite_test_filename = os.path.join(tmp_dir, "composite_primary_key.sqlite")
dbname = "composite_primary_key_db"
b_schema = "myschema"

sql_modify_fkey = """
    ALTER TABLE myschema.referencing
    DROP CONSTRAINT referencing_fkid1_fkey;

    ALTER TABLE myschema.referencing
    ADD CONSTRAINT referencing_fkid1_fkey
    FOREIGN KEY (fkid1,fkid2)
    REFERENCES myschema.referenced(id1,id2)
    {ftype};
    """


class BaseTest:

    def __init__(self, host, pguser, schema, additional_sql=None):

        self.schema = schema
        self.cur = None
        self.con = None
        self.versioning = None

        self.pg_conn_info = "dbname={} host={} user={}".format(
            dbname, host, pguser)

        test_data_dir = os.path.dirname(os.path.realpath(__file__))
        sql_file = os.path.join(test_data_dir, "composite_primary_key_db.sql")

        # create the test database
        os.system(f"dropdb --if-exists -h {host} -U {pguser} {dbname}")
        os.system(f"createdb -h {host} -U {pguser} {dbname}")
        os.system(f"psql -h {host} -U {pguser} {dbname} -f {sql_file}")

        self.pcon = psycopg2.connect(self.pg_conn_info)
        self.pcur = self.pcon.cursor()

        if additional_sql:
            self.pcur.execute(additional_sql)
            self.pcon.commit()

        versioning.historize(
            "dbname={} host={} user={}".format(dbname, host, pguser), b_schema)

    def commit_and_check(self, to_check):
        """
        Commit and check that given table has the given expected rows number
        """
        # if there is some remaining instruction (like select has been done
        # and not committed) it block the commit because commit method
        # cursor try to modify a table our cursor is currently pointing
        # on. So we rollback to be sure!
        self.con.rollback()

        for table, nb in to_check:
            self.versioning.commit("commit msg")
            self.pcur.execute("SELECT COUNT(*) FROM {}.{}".format(b_schema,
                                                                  table))
            res = self.pcur.fetchone()[0]
            assert (res == nb),\
                "Expected {} rows for {}, got {}".format(nb, table, res)

    def test_insert(self):

        self.cur.execute("SELECT COUNT(*) FROM {}.referenced_view".format(
            self.schema))
        assert(self.cur.fetchone()[0] == 2)

        self.cur.execute("SELECT COUNT(*) FROM {}.referencing_view".format(
            self.schema))
        assert(self.cur.fetchone()[0] == 1)

        # insert valid
        self.cur.execute(
            "insert into {}.referencing_view (id, fkid1, fkid2) "
            "values (18,42,4);".format(self.schema))

        self.con.commit()
        self.cur.execute("SELECT COUNT(*) FROM {}.referencing_view".format(
            self.schema))
        assert(self.cur.fetchone()[0] == 2)

        # insert fail unique constraint
        try:
            self.cur.execute(
                "insert into {}.referencing_view (id, fkid1, fkid2) "
                "values (16,1,18);".format(self.schema))
            assert(False and "Insert must fail unique constraint")
        except (IntegrityError, psycopg2.InternalError):
            self.con.rollback()
        else:
            self.con.commit()

        # insert fail foreign key constraint
        try:
            self.cur.execute(
                "insert into {}.referencing_view (id, fkid1, fkid2) "
                "values (19,42,7);".format(self.schema))
            assert(False and "Insert must fail foreign key constraint")
        except (IntegrityError, psycopg2.InternalError):
            self.con.rollback()
        else:
            self.con.commit()

        # 1 existing feature, insert one, so 2 expected revisions
        self.commit_and_check([("referencing", 2)])

    def test_update_referencing(self):

        # insert one more referencing for testing
        self.cur.execute("insert into {}.referencing_view "
                         "(id, fkid1, fkid2) "
                         "values (17,42,4);".format(self.schema))
        self.con.commit()
        self.cur.execute("SELECT COUNT(*) FROM {}.referencing_view".format(
            self.schema))
        assert(self.cur.fetchone()[0] == 2)

        # update nothing to do with foreign key or unique constraint
        self.cur.execute(
            "UPDATE {}.referencing_view SET name = 'X' WHERE id = 16".format(
                self.schema))
        self.con.commit()
        self.cur.execute(
            "SELECT name FROM {}.referencing_view WHERE id = 16".format(
                self.schema))
        assert(self.cur.fetchone()[0] == 'X')

        # update valid unique constraint
        self.cur.execute(
            "UPDATE {}.referencing_view SET id = 18 WHERE id = 16".format(
                self.schema))
        self.con.commit()
        self.cur.execute(
            "SELECT * FROM {}.referencing_view WHERE id = 18".format(
                self.schema))
        assert(len(self.cur.fetchall()) == 1)

        # update valid foreign key constraint
        self.cur.execute(
            "UPDATE {}.referencing_view "
            "SET fkid1 = 1, fkid2 = 18 WHERE id = 17".format(
                self.schema))
        self.con.commit()
        self.cur.execute(
            "SELECT fkid1, fkid2 FROM {}.referencing_view "
            "WHERE id = 17".format(
                self.schema))
        assert(self.cur.fetchone() == (1, 18))

        # update fail unique constraint
        try:
            self.cur.execute(
                "UPDATE {}.referencing_view SET ID = 17 WHERE id = 18".format(
                    self.schema))
            assert(False and "Insert must fail unique constraint")
        except (IntegrityError, psycopg2.InternalError):
            self.con.rollback()
        else:
            self.con.commit()

        # update fail foreign key constraint
        try:
            self.cur.execute("UPDATE {}.referencing_view "
                             "SET fkid1 = 3, fkid2 = 18 "
                             "WHERE id = 17".format(self.schema))
            assert(False and "Update must fail foreign key constraint")
        except (IntegrityError, psycopg2.InternalError):
            self.con.rollback()
        else:
            self.con.commit()

        # 1 existing feature, insert and one, modify several times one so 3
        # revisions
        self.commit_and_check([("referencing", 3)])

    def test_delete_restrict(self):

        # delete is restrict, must fail
        try:
            self.cur.execute(
                "DELETE FROM {}.referenced_view "
                "WHERE id1 = 1 and id2 = 18".format(
                    self.schema))
            assert(False and "Delete must fail because of referenced key")
        except (IntegrityError, psycopg2.InternalError):
            self.con.rollback()
        else:
            self.con.commit()

        # Two existing feature, delete has failed, so 2 revisions
        self.commit_and_check([("referenced", 2)])

    def test_delete_cascade(self):

        self.cur.execute(
            "DELETE FROM {}.referenced_view WHERE id1 = 1 and id2 = 18".format(
                self.schema))
        self.con.commit()

        self.cur.execute("SELECT * FROM {}.referenced_view".format(
            self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute("SELECT * FROM {}.referencing_view".format(
            self.schema))
        assert(len(self.cur.fetchall()) == 0)

        # 2 referenced, delete one (modify its rev end field, so 2 revisions
        # 1 referencing, cascade deleted (modify its rev end field)
        # so 1 revision
        self.commit_and_check([("referenced", 2), ("referencing", 1)])

    def test_update_cascade(self):

        self.cur.execute(
            "UPDATE {}.referenced_view SET id1 = 2, id2 = 7 "
            "WHERE id1 = 1 AND id2 = 18".format(
                self.schema))
        self.con.commit()
        self.cur.execute(
            "SELECT count(*) FROM {}.referenced_view "
            "WHERE id1 = 2 AND id2 = 7".format(
                self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute(
            "SELECT * FROM {}.referencing_view "
            "WHERE fkid1 = 2 AND fkid2 = 7".format(
                self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute(
            "SELECT * FROM {}.referencing_view "
            "WHERE fkid1 = 1 AND fkid2 = 18".format(
                self.schema))
        assert(len(self.cur.fetchall()) == 0)

        # 2 referenced, update one, so 3 revisions
        # 1 referencing, cascade updated so 2 revision
        self.commit_and_check([("referenced", 3), ("referencing", 2)])

    def test_delete_setnull(self):

        self.cur.execute(
            "DELETE FROM {}.referenced_view "
            "WHERE id1 = 1 and id2 = 18".format(self.schema))
        self.con.commit()

        self.cur.execute(
            "SELECT * FROM {}.referenced_view".format(self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute(
            "SELECT fkid1, fkid2 FROM {}.referencing_view".format(self.schema))
        referencing = self.cur.fetchall()
        assert(len(referencing) == 1)
        assert(referencing[0][0] is None)
        assert(referencing[0][1] is None)

        # 2 referenced, delete one (modify its rev_end field), so 2 revisions
        # 1 referencing, cascade updated so 2 revisions
        self.commit_and_check([("referenced", 2), ("referencing", 2)])

    def test_update_setnull(self):

        self.cur.execute(
            "UPDATE {}.referenced_view SET id1 = 7, id2 = 15 "
            "WHERE id1 = 1 and id2 = 18".format(self.schema))
        self.con.commit()
        self.cur.execute(
            "SELECT count(*) FROM {}.referenced_view "
            "WHERE id1 = 7 AND id2 = 15".format(self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute(
            "SELECT fkid1, fkid2 FROM {}.referencing_view".format(self.schema))
        referencing = self.cur.fetchall()
        assert(len(referencing) == 1)
        assert(referencing[0][0] is None)
        assert(referencing[0][1] is None)

        # 2 referenced, update one, so 3 revisions
        # 1 referencing, cascade updated so 2 revisions
        self.commit_and_check([("referenced", 3), ("referencing", 2)])

    def test_delete_setdefault(self):

        self.cur.execute(
            "DELETE FROM {}.referenced_view WHERE id1 = 1 and id2 = 18".format(
                self.schema))
        self.con.commit()

        self.cur.execute(
            "SELECT * FROM {}.referenced_view".format(self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute(
            "SELECT fkid1, fkid2 FROM {}.referencing_view".format(self.schema))
        referencing = self.cur.fetchall()
        assert(len(referencing) == 1)
        assert(referencing[0][0] == 42)
        assert(referencing[0][1] == 4)

        # 2 referenced, delete one (modify its rev_end), so 2 revisions
        # 1 referencing, cascade updated so 2 revisions
        self.commit_and_check([("referenced", 2), ("referencing", 2)])

    def test_update_setdefault(self):

        self.cur.execute(
            "UPDATE {}.referenced_view SET id1 = 7, id2 = 15 "
            "WHERE id1 = 1 and id2 = 18".format(self.schema))
        self.con.commit()
        self.cur.execute(
            "SELECT count(*) FROM {}.referenced_view "
            "WHERE id1 = 7 and id2 = 15".format(self.schema))
        assert(len(self.cur.fetchall()) == 1)

        self.cur.execute(
            "SELECT fkid1, fkid2 FROM {}.referencing_view".format(self.schema))
        referencing = self.cur.fetchall()
        assert(len(referencing) == 1)
        assert(referencing[0][0] == 42)
        assert(referencing[0][1] == 4)

        # 2 referenced, update one, so 3 revisions
        # 1 referencing, cascade updated so 2 revisions
        self.commit_and_check([("referenced", 3), ("referencing", 2)])


class SpatialiteTest(BaseTest):

    def __init__(self, host, pguser, additional_sql=None):

        super().__init__(host, pguser, "main", additional_sql)

        if os.path.isfile(sqlite_test_filename):
            os.remove(sqlite_test_filename)

        self.versioning = versioning.spatialite(sqlite_test_filename,
                                                self.pg_conn_info)
        self.versioning.checkout(["myschema_trunk_rev_head.referencing",
                                  "myschema_trunk_rev_head.referenced"])

        self.con = dbapi2.connect(sqlite_test_filename)
        self.con.enable_load_extension(True)
        self.con.execute("SELECT load_extension('mod_spatialite')")
        self.cur = self.con.cursor()

    def __del__(self):
        if self.con:
            self.con.close()

        if self.pcon:
            self.pcon.close()


class PgServerTest(BaseTest):

    def __init__(self, host, pguser, additional_sql=None):

        wc_schema = "myschema_workingcopy"
        super().__init__(host, pguser, wc_schema, additional_sql)

        self.versioning = versioning.pgServer(self.pg_conn_info,
                                              wc_schema)
        self.versioning.checkout(["myschema_trunk_rev_head.referencing",
                                  "myschema_trunk_rev_head.referenced"])

        self.con = self.pcon
        self.cur = self.pcur


class PgLocalTest(BaseTest):

    def __init__(self, host, pguser, additional_sql=None):

        wc_schema = "myschema_workingcopy"
        super().__init__(host, pguser, wc_schema, additional_sql)

        db_name = dbname + "_wc"
        pg_conn_info_out = "dbname={} host={} user={}".format(
            db_name, host, pguser)

        # create the test working copy database
        os.system(f"dropdb --if-exists -h {host} -U {pguser} {db_name}")
        os.system(f"createdb -h {host} -U {pguser} {db_name}")
        os.system(f"psql -h {host} -U {pguser} {db_name}"
                  " -c 'CREATE EXTENSION postgis;'")

        self.versioning = versioning.pgLocal(self.pg_conn_info,
                                             wc_schema, pg_conn_info_out)
        self.versioning.checkout(["myschema_trunk_rev_head.referencing",
                                  "myschema_trunk_rev_head.referenced"])

        self.con = psycopg2.connect(pg_conn_info_out)
        self.cur = self.con.cursor()


def test(host, pguser):

    # loop on the 3 ways of checkout (sqlite, pgserver, pglocal)
    for test_class in [SpatialiteTest,
                       PgServerTest,
                       PgLocalTest]:

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
            ftype="ON UPDATE CASCADE"))
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
        sql += """ALTER TABLE myschema.referencing ALTER COLUMN fkid1
        SET DEFAULT 42;"""
        sql += """ALTER TABLE myschema.referencing ALTER COLUMN fkid2
        SET DEFAULT 4;"""
        test = test_class(host, pguser, sql)
        test.test_delete_setdefault()
        del test

        sql = sql_modify_fkey.format(ftype="ON UPDATE SET DEFAULT")
        sql += """ALTER TABLE myschema.referencing ALTER COLUMN fkid1
        SET DEFAULT 42;"""
        sql += """ALTER TABLE myschema.referencing ALTER COLUMN fkid2
        SET DEFAULT 4;"""
        test = test_class(host, pguser, sql)
        test.test_update_setdefault()
        del test


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 constraints_test.py host pguser")
    else:
        test(*sys.argv[1:])
