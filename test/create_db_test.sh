#!/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f $0))

for db in epanet_test_db epanet_test_db_copy;
do
    dropdb --if-exists -h 127.0.01 -U postgres $db
    createdb -h 127.0.01 -U postgres $db
    psql -h 127.0.01 -U postgres $db -c 'CREATE EXTENSION postgis'
    psql -h 127.0.01 -U postgres $db -f $SCRIPT_DIR/epanet_test_db_wo_versioning.sql
done

EMPTY_DB="qgis_versioning_empty_db"
dropdb --if-exists -h 127.0.01 -U postgres $EMPTY_DB
createdb -h 127.0.01 -U postgres $EMPTY_DB
psql -h 127.0.01 -U postgres $EMPTY_DB -c 'CREATE EXTENSION postgis'

