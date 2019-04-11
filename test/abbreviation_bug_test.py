#!/usr/bin/env python3

import sys
from versioningDB import versioning
from sqlite3 import dbapi2
import psycopg2
import os
import tempfile

longname = '_this_is_a_very_long_name_that should_be_trunctated_if_buggy'
another_longname = ('this_is_another_edited_very_long_name_that '
                    'should_be_trunctated_if_buggy')
new_longname = 'newly inserted with long name'


def test(host, pguser):
    pg_conn_info = "dbname=epanet_test_db host=" + host + " user=" + pguser
    tmp_dir = tempfile.gettempdir()
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    sqlite_test_filename = tmp_dir+"/abbreviation_test.sqlite"
    if os.path.isfile(sqlite_test_filename):
        os.remove(sqlite_test_filename)

    spversioning = versioning.spatialite(sqlite_test_filename, pg_conn_info)
    # create the test database
    os.system("dropdb --if-exists -h " + host + " -U "+pguser
              + " epanet_test_db")
    os.system("createdb -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -f "
              + test_data_dir + "/epanet_test_db.sql")

    # delete existing data
    pcon = psycopg2.connect(pg_conn_info)
    pcur = pcon.cursor()
    for i in range(10):
        pcur.execute("""
        INSERT INTO epanet.junctions
        (demand_pattern_id, elevation, geom)
        VALUES
        ('{demand_pattern_id}', {elev},
        ST_GeometryFromText('POINT({x} {y})',2154));
        """.format(
            demand_pattern_id=str(i+2)
            + longname,
            elev=float(i),
            x=float(i+1),
            y=float(i+1)
        ))
    pcon.commit()
    versioning.historize(pg_conn_info, 'epanet')

    spversioning.checkout(["epanet_trunk_rev_head.junctions",
                           "epanet_trunk_rev_head.pipes"])
    assert(os.path.isfile(sqlite_test_filename)
           and "sqlite file must exist at this point")

    scon = dbapi2.connect(sqlite_test_filename)
    scon.enable_load_extension(True)
    scon.execute("SELECT load_extension('mod_spatialite')")
    scur = scon.cursor()
    scur.execute("SELECT id, demand_pattern_id from junctions")

    for rec in scur:
        if rec[0] > 2:
            assert rec[1].find(longname) != -1

    scur.execute(f"""
    update junctions_view
    set demand_pattern_id='{another_longname}' where ogc_fid > 8""")

    scur.execute(f"""
    insert into junctions_view(id, demand_pattern_id, elevation, geom)
    select 13, '{new_longname}', elevation, geom
    from junctions_view where ogc_fid=4""")
    scon.commit()

    spversioning.commit('a commit msg')

    pcur.execute("""select versioning_id, demand_pattern_id
    from epanet_trunk_rev_head.junctions""")
    for row in pcur:
        print(row)
        if row[0] > 8:
            assert row[1].find(another_longname) != -1\
                or row[1].find(new_longname) != -1


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 versioning_base_test.py host pguser")
    else:
        test(*sys.argv[1:])
