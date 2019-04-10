#!/usr/bin/env python3

import sys
sys.path.insert(0, '..')

from versioningDB import versioning
from sqlite3 import dbapi2
import psycopg2
import os
import tempfile

tmp_dir = tempfile.gettempdir()
sqlite_test_filename = os.path.join(tmp_dir, "partial_checkout_test.sqlite")


class PartialCheckoutTest:

    def __init__(self, host, pguser, schema):

        self.schema = schema
        self.cur = None
        self.con = None
        self.versioning = None

        self.pg_conn_info = f"dbname=epanet_test_db host={host} user={pguser}"
        self.pg_conn_info_cpy = f"dbname=epanet_test_copy_db host={host} user={pguser}"

        test_data_dir = os.path.dirname(os.path.realpath(__file__))

        # create the test database
        os.system(f"dropdb --if-exists -h {host} -U {pguser} epanet_test_db")
        os.system(f"dropdb --if-exists -h {host} -U {pguser} epanet_test_copy_db")
        os.system("createdb -h " + host + " -U "+pguser+" epanet_test_db")
        os.system("createdb -h " + host + " -U "+pguser+" epanet_test_copy_db")
        os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -f "
                  + test_data_dir + "/epanet_test_db.sql")

        self.pcon = psycopg2.connect(self.pg_conn_info)
        self.pcur = self.pcon.cursor()
        for i in range(10):
            self.pcur.execute("""
            INSERT INTO epanet.junctions
            (id, elevation, geom)
            VALUES
            ('{id}', {elev}, ST_GeometryFromText('POINT({x} {y})',2154));
            """.format(
                id=i+3,
                elev=float(i),
                x=float(i+1),
                y=float(i+1)
            ))
        self.pcon.commit()

        versioning.historize(self.pg_conn_info, 'epanet')

    def __del__(self):
        if self.con:
            self.con.close()

        if self.pcon:
            self.pcon.close()

    def checkout(self, tables, feature_list):
        self.versioning.checkout(tables, feature_list)

    def test_select(self):

        self.checkout(["epanet_trunk_rev_head.junctions",
                       "epanet_trunk_rev_head.pipes"], [[1, 2, 3], []])

        self.cur.execute("SELECT elevation from {}.junctions_view".format(
            self.schema))
        assert([res[0] for res in self.cur.fetchall()] == [0., 1., 0.])

    def test_referenced(self):
        """ checkout table, its referenced table and the referenced
        features must appear"""

        self.checkout(["epanet_trunk_rev_head.pipes"], [[1]])
        self.cur.execute("SELECT id from {}.pipes_view order by id".format(
            self.schema))
        assert([res[0] for res in self.cur.fetchall()] == [1])

        self.cur.execute("SELECT id from {}.junctions_view order by id".format(
            self.schema))
        assert([res[0] for res in self.cur.fetchall()] == [1, 2])

    def test_referenced_union(self):
        """ checkout table, its referenced table and the referenced
        features must appear, plus the one already selected"""

        self.checkout(["epanet_trunk_rev_head.pipes",
                       "epanet_trunk_rev_head.junctions"], [[1], [6, 7]])
        self.cur.execute("SELECT id from {}.pipes_view order by id".format(
            self.schema))
        assert([res[0] for res in self.cur.fetchall()] == [1])

        self.cur.execute("SELECT id from {}.junctions_view order by id".format(
            self.schema))
        assert([res[0] for res in self.cur.fetchall()] == [1, 2, 6, 7])

    def test_referencing(self):
        """ checkout table, its referencing table and the referencing
        features must appear"""

        self.checkout(["epanet_trunk_rev_head.junctions"], [[1, 6, 7]])
        self.cur.execute("SELECT id from {}.junctions_view order by id".format(
            self.schema))
        assert([res[0] for res in self.cur.fetchall()] == [1, 6, 7])

        self.cur.execute("SELECT id from {}.pipes_view order by id".format(
            self.schema))
        assert([res[0] for res in self.cur.fetchall()] == [1])

    def test_duplicate_pkey(self):

        self.checkout(["epanet_trunk_rev_head.junctions"], [[6, 7, 8]])
        self.cur.execute("SELECT id from {}.junctions_view order by id".format(
            self.schema))
        assert([res[0] for res in self.cur.fetchall()] == [6, 7, 8])

        self.cur.execute("""
        INSERT INTO {}.junctions_view (id, elevation, geom) VALUES
        (4, 40, ST_GeometryFromText('POINT(4 4)',2154)),
        (5, 50, ST_GeometryFromText('POINT(5 5)',2154))""".format(
            self.schema))
        self.con.commit()

        self.cur.execute("SELECT id from {}.junctions_view order by id".format(
            self.schema))
        assert([res[0] for res in self.cur.fetchall()] == [4, 5, 6, 7, 8])

        self.con.rollback()

        try:
            self.versioning.commit("commit msg")
            assert(False and "Commit must fail unique constraint")
        except RuntimeError as e:
            print(e)
            self.con.rollback()

        # Check we have only one current instance of feature with id 5
        # and one with id 4 after commit
        self.pcur.execute("""SELECT COUNT(*)
        FROM epanet.junctions WHERE id = 5 AND trunk_rev_end IS NULL """)
        assert(self.pcur.fetchone()[0] == 1)

        self.pcur.execute("""SELECT COUNT(*)
        FROM epanet.junctions WHERE id = 4 AND trunk_rev_end IS NULL """)
        assert(self.pcur.fetchone()[0] == 1)


class SpatialitePartialCheckoutTest(PartialCheckoutTest):

    def __init__(self, host, pguser):
        super().__init__(host, pguser, "main")

        if os.path.isfile(sqlite_test_filename):
            os.remove(sqlite_test_filename)

        self.versioning = versioning.spatialite(sqlite_test_filename,
                                                self.pg_conn_info)

    def checkout(self, tables, feature_list):

        super().checkout(tables, feature_list)

        self.con = dbapi2.connect(sqlite_test_filename)
        self.con.enable_load_extension(True)
        self.con.execute("SELECT load_extension('mod_spatialite')")
        self.cur = self.con.cursor()


class PgServerPartialCheckoutTest(PartialCheckoutTest):

    def __init__(self, host, pguser):

        wc_schema = "epanet_workingcopy"
        super().__init__(host, pguser, wc_schema)

        self.versioning = versioning.pgServer(self.pg_conn_info,
                                              wc_schema)

    def checkout(self, tables, feature_list):
        super().checkout(tables, feature_list)
        self.con = self.pcon
        self.cur = self.pcur


class PgLocalPartialCheckoutTest(PartialCheckoutTest):

    def __init__(self, host, pguser):

        wc_schema = "epanet_workingcopy"
        super().__init__(host, pguser, wc_schema)

        self.versioning = versioning.pgLocal(
            self.pg_conn_info, wc_schema, self.pg_conn_info_cpy)

    def checkout(self, tables, feature_list):
        super().checkout(tables, feature_list)

        self.con = psycopg2.connect(self.pg_conn_info_cpy)
        self.cur = self.con.cursor()


def test(host, pguser):

    # loop on the 3 ways of checkout (sqlite, pgserver, pglocal)
    for test_class in [SpatialitePartialCheckoutTest,
                       PgLocalPartialCheckoutTest,
                       PgServerPartialCheckoutTest
    ]:

        test = test_class(host, pguser)
        test.test_select()
        del test

        test = test_class(host, pguser)
        test.test_referenced()
        del test

        test = test_class(host, pguser)
        test.test_referenced_union()
        del test

        test = test_class(host, pguser)
        test.test_referencing()
        del test

        test = test_class(host, pguser)
        test.test_duplicate_pkey()
        del test


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 versioning_base_test.py host pguser")
    else:
        test(*sys.argv[1:])
