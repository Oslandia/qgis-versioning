#!/usr/bin/python

from .. import versioning
from pyspatialite import dbapi2
import psycopg2
import os
import shutil

if __name__ == "__main__":
    test_data_dir = os.path.dirname(os.path.realpath(__file__))
    tmp_dir = "/tmp"

    # create the test database

    os.system("dropdb epanet_test_db")
    os.system("createdb epanet_test_db")
    os.system("psql epanet_test_db -c 'CREATE EXTENSION postgis'")

    pg_conn_info = "dbname=epanet_test_db"
    pcur = versioning.Db(psycopg2.connect(pg_conn_info))
    pcur.execute("CREATE SCHEMA epanet")
    pcur.execute("""
        CREATE TABLE epanet.junctions (
            hid serial PRIMARY KEY,
            id varchar,
            elevation float,
            base_demand_flow float,
            demand_pattern_id varchar,
            geom geometry('POINT',2154)
        )""")

    pcur.execute("""
        INSERT INTO epanet.junctions
            (id, elevation, geom)
            VALUES
            ('0',0,ST_GeometryFromText('POINT(1 0)',2154))""")

    pcur.execute("""
        INSERT INTO epanet.junctions
            (id, elevation, geom)
            VALUES
            ('1',1,ST_GeometryFromText('POINT(0 1)',2154))""")

    pcur.execute("""
        CREATE TABLE epanet.pipes (
            hid serial PRIMARY KEY,
            id varchar,
            start_node varchar,
            end_node varchar,
            length float,
            diameter float,
            roughness float,
            minor_loss_coefficient float,
            status varchar,
            geom geometry('LINESTRING',2154)
        )""")

    pcur.execute("""
        INSERT INTO epanet.pipes
            (id, start_node, end_node, length, diameter, geom)
            VALUES
            ('0','0','1',1,2,ST_GeometryFromText('LINESTRING(1 0,0 1)',2154))""")

    pcur.commit()
    pcur.close()

    versioning.historize( pg_conn_info, 'epanet' )

    failed = False
    try:
        versioning.add_branch( pg_conn_info, 'epanet', 'trunk' )
    except:
        failed = True
    assert( failed )

    failed = False
    try:
        versioning.add_branch( pg_conn_info, 'epanet', 'mybranch', 'message', 'toto' )
    except:
        failed = True
    assert( failed )

    versioning.add_branch( pg_conn_info, 'epanet', 'mybranch', 'test msg' )


    pcur = versioning.Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT * FROM epanet_mybranch_rev_head.junctions")
    assert( len(pcur.fetchall()) == 2 )
    pcur.execute("SELECT * FROM epanet_mybranch_rev_head.pipes")
    assert( len(pcur.fetchall()) == 1 )

    ##versioning.add_revision_view( pg_conn_info, 'epanet', 'mybranch', 2)
    ##pcur.execute("SELECT * FROM epanet_mybranch_rev_2.junctions")
    ##assert( len(pcur.fetchall()) == 2 )
    ##pcur.execute("SELECT * FROM epanet_mybranch_rev_2.pipes")
    ##assert( len(pcur.fetchall()) == 1 )

    select_str, where_str =  versioning.rev_view_str( pg_conn_info, 'epanet', 'junctions','mybranch', 2)
    pcur.execute(select_str + " WHERE " + where_str)
    assert( len(pcur.fetchall()) == 2 )
    select_str, where_str =  versioning.rev_view_str( pg_conn_info, 'epanet', 'pipes','mybranch', 2)
    pcur.execute(select_str + " WHERE " + where_str)
    assert( len(pcur.fetchall()) == 1 )

    pcur.close()
