#!/usr/bin/python
import versioning_base
import psycopg2
import os
import shutil

test_data_dir = os.path.dirname(os.path.realpath(__file__))

# create the test database

os.system("dropdb epanet_test_db")
os.system("createdb epanet_test_db")
os.system("psql epanet_test_db -c 'CREATE EXTENSION postgis'")
os.system("psql epanet_test_db -f "+test_data_dir+"/../html/epanet_test_db.sql")

# chechout
versioning_base.pg_checkout("dbname=epanet_test_db",['epanet_trunk_rev_head.junctions','epanet_trunk_rev_head.pipes'], "epanet_working_copy")


