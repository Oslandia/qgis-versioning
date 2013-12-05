#!/bin/sh

die()
{
    echo "error: " $1 1>&2
    exit 1
}

dropdb valcea || die "cannot drop db"
createdb valcea || die "cannot create db"

psql valcea < epanet_test_db.sql || die "cannot load test db"

# posgresql -> spatialite
rm valcea.sqlite
ogr2ogr -preserve_fid -f SQLite -dsco SPATIALITE=yes valcea.sqlite PG:"dbname='valcea' active_schema=epanet"  junctions || die "cannot convert junctions to spatialite"
ogr2ogr -preserve_fid -f SQLite  -update valcea.sqlite PG:"dbname='valcea' active_schema=epanet" pipes || die "cannot convert pipes to spatialite"

# working offline
cat spatialite_triggers_and_views.sql | spatialite valcea.sqlite || die "cannot work offline"

# spatialite -> posgresql
psql valcea -c 'CREATE SCHEMA epanet_test;' || die "cannot create schema"
ogr2ogr -f PostgreSQL PG:"dbname='valcea' active_schema=epanet_test host='localhost' port='5432' user='vmo' password='toto'" -lco LAUNDER="YES" -lco GEOMETRY_NAME=geom -lco FID=hid valcea.sqlite || die "cannot convert spatialite to postgis"


# some columns need renaming
# ALTER TABLE epanet_test.junctions RENAME ogc_fid TO gid;
# ALTER TABLE epanet_test.junctions RENAME wkb_geometry TO geom;

#echo "SELECT sql FROM sqlite_master where tbl_name='junctions';" | spatialite valcea.sqlite

#echo "SELECT * FROM junctions;" | spatialite valcea.sqlite
#echo "SELECT * FROM junctions;" | spatialite valcea.sqlite

#psql valcea -c 'SELECT * FROM epanet_test.junctions;'

psql valcea -f merge.sql || die "cannot merge modifications"
