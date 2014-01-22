#!/usr/bin/python
import versioning_base
from pyspatialite import dbapi2
import psycopg2
import os
import shutil

test_data_dir = os.path.dirname(os.path.realpath(__file__))
tmp_dir = "/tmp"

# create the test database

os.system("dropdb epanet_test_db")
os.system("createdb epanet_test_db")
os.system("psql epanet_test_db -c 'CREATE EXTENSION postgis'")
os.system("psql epanet_test_db -f "+test_data_dir+"/issue287_pg_dump.sql")

# try the update
shutil.copyfile(test_data_dir+"/issue287_wc.sqlite", tmp_dir+"/issue287_wc.sqlite")
versioning_base.update(tmp_dir+"/issue287_wc.sqlite")
