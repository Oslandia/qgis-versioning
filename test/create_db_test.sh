#!/usr/bin/env bash

SCRIPT_DIR=$(dirname $(readlink -f $0))

for db in epanet_test_db composite_primary_key_db;
do
    dropdb --if-exists -h 127.0.01 -U postgres $db
    createdb -h 127.0.01 -U postgres $db
    psql -h 127.0.01 -U postgres $db -f $SCRIPT_DIR/$db.sql
done

EMPTY_DB="qgis_versioning_empty_db"
dropdb --if-exists -h 127.0.01 -U postgres $EMPTY_DB
createdb -h 127.0.01 -U postgres $EMPTY_DB
psql -h 127.0.01 -U postgres $EMPTY_DB -c 'CREATE EXTENSION postgis'

