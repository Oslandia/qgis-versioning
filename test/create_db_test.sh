#!/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f $0))

for db in epanet_test_db epanet_test_db_copy;
do
    dropdb --if-exists -h 127.0.01 -U julien $db
    createdb -h 127.0.01 -U julien $db
    psql -h 127.0.01 -U julien $db -c 'CREATE EXTENSION postgis'
    psql -h 127.0.01 -U julien $db -f $SCRIPT_DIR/epanet_test_db.sql
done

EMPTY_DB="qgis_versioning_empty_db"
dropdb --if-exists -h 127.0.01 -U julien $EMPTY_DB
createdb -h 127.0.01 -U julien $EMPTY_DB
psql -h 127.0.01 -U julien $EMPTY_DB -c 'CREATE EXTENSION postgis'

