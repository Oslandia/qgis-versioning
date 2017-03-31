#!/usr/bin/python
from .. import versioning
from pyspatialite import dbapi2
import psycopg2
import os
import shutil
import tempfile

if __name__ == "__main__":
    test_data_dir = os.path.dirname(os.path.realpath(__file__))
    tmp_dir = tempfile.gettempdir()

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
            printmap integer[],
            geometry geometry('POINT',2154),
            geometry_schematic geometry('POLYGON',2154)
        )""")

    pcur.execute("""
        INSERT INTO epanet.junctions
            (id, elevation, printmap, geometry, geometry_schematic)
            VALUES
            ('0',0,'{1,2,3}',ST_GeometryFromText('POINT(0 0)',2154),
            ST_GeometryFromText('POLYGON((-1 -1,1 -1,1 1,-1 1,-1 -1))',2154))""")

    pcur.execute("""
        INSERT INTO epanet.junctions
            (id, elevation, printmap, geometry, geometry_schematic)
            VALUES
            ('1',1,'{}',ST_GeometryFromText('POINT(0 1)',2154),
            ST_GeometryFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))',2154))""")

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
            geometry geometry('LINESTRING',2154)
        )""")

    pcur.execute("""
        INSERT INTO epanet.pipes
            (id, start_node, end_node, length, diameter, geometry) 
            VALUES
            ('0','0','1',1,2,ST_GeometryFromText('LINESTRING(1 0,0 1)',2154))""")

    pcur.commit()
    pcur.close()

    versioning.historize( pg_conn_info, 'epanet' )

    pcur = versioning.Db(psycopg2.connect(pg_conn_info))

    pcur.execute("SELECT ST_AsText(geometry), ST_AsText(geometry_schematic) FROM epanet_trunk_rev_head.junctions")
    res = pcur.fetchall()
    assert( res[0][0] == 'POINT(0 0)' )
    assert( res[1][1] == 'POLYGON((0 0,2 0,2 2,0 2,0 0))' )


    wc = tmp_dir+'/wc_multiple_geometry_test.sqlite'
    if os.path.isfile(wc): os.remove(wc) 
    versioning.checkout( pg_conn_info, ['epanet_trunk_rev_head.pipes','epanet_trunk_rev_head.junctions'], wc )


    scur = versioning.Db( dbapi2.connect(wc) )
    scur.execute("UPDATE junctions_view SET GEOMETRY = GeometryFromText('POINT(3 3)',2154) WHERE OGC_FID = 1")
    scur.commit()
    scur.execute("SELECT * from junctions_view")
    print "--------------"
    for res in scur.fetchall(): print res
    scur.close()
    versioning.commit( wc, 'moved a junction', 'dbname=epanet_test_db' )

    pcur.execute("SELECT ST_AsText(geometry), ST_AsText(geometry_schematic), printmap FROM epanet_trunk_rev_head.junctions ORDER BY hid DESC")
    res = pcur.fetchall()
    for r in res: print r
    assert( res[0][0] == 'POINT(3 3)' )
    assert( res[0][1] == 'POLYGON((-1 -1,1 -1,1 1,-1 1,-1 -1))' )
    assert( res[0][2] == [1, 2, 3] )

    pcur.close()

