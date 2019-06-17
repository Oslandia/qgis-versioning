#!/usr/bin/env python3

import sys
from versioningDB import versioning
from sqlite3 import dbapi2
import os
import tempfile
import psycopg2


def test(host, pguser):

    dbname = "epanet_test_db"
    test_data_dir = os.path.dirname(os.path.realpath(__file__))
    sql_file = os.path.join(test_data_dir, "epanet_test_db.sql")
    tmp_dir = tempfile.gettempdir()

    # create the test database
    os.system(f"dropdb --if-exists -h {host} -U {pguser} {dbname}")
    os.system(f"createdb -h {host} -U {pguser} {dbname}")
    os.system(f"psql -h {host} -U {pguser} {dbname} -f {sql_file}")

    pg_conn_info = f"dbname={dbname} host={host} user={pguser}"

    pcon = psycopg2.connect(pg_conn_info)
    pcur = pcon.cursor()
    pcur.execute("CREATE TYPE type_example AS ENUM('TEST1', 'TEST2')")
    pcur.execute("ALTER TABLE epanet.junctions "
                 "ADD COLUMN type_field type_example;")
    pcon.commit()

    versioning.historize(pg_conn_info, "epanet")

    # try the update
    wc = tmp_dir+"/issue_type.sqlite"
    if os.path.isfile(wc):
        os.remove(wc)

    spversioning = versioning.spatialite(wc, pg_conn_info)
    spversioning.checkout(['epanet_trunk_rev_head.junctions'])

    scur = versioning.Db(dbapi2.connect(wc))

    # scur.execute("SELECT * FROM pipes")
    # assert( len(scur.fetchall()) == 1 )
    scur.execute("UPDATE junctions_view "
                 "SET type_field = 'TEST1' WHERE OGC_FID = 1")
    scur.commit()

    spversioning.commit("test type")

    pcon = psycopg2.connect(pg_conn_info)
    pcur = pcon.cursor()
    pcur.execute("SELECT type_field FROM epanet.junctions "
                 "WHERE id = 1 AND trunk_rev_end IS NULL")

    res = pcur.fetchall()
    assert(len(res) == 1)
    assert(res[0][0] == "TEST1")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 issue_type_test.py host pguser")
    else:
        test(*sys.argv[1:])
