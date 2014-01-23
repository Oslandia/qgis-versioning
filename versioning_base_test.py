#!/usr/bin/python
import versioning_base
from pyspatialite import dbapi2
import psycopg2
import os

sqlite_test_filename1 = "versioning_base_test1.sqlite"
sqlite_test_filename2 = "versioning_base_test2.sqlite"
if os.path.isfile(sqlite_test_filename1): os.remove(sqlite_test_filename1)
if os.path.isfile(sqlite_test_filename2): os.remove(sqlite_test_filename2)

# create the test database

os.system("dropdb epanet_test_db")
os.system("createdb epanet_test_db")
os.system("psql epanet_test_db -c 'CREATE EXTENSION postgis'")
os.system("psql epanet_test_db -f html/epanet_test_db.sql")

# chechout two tables

try:
    versioning_base.checkout("dbname=epanet_test_db",["epanet_trunk_rev_head.junctions","epanet.pipes"], sqlite_test_filename1) 
    assert(False and "checkout from schema withouti suffix _branch_rev_head should not be successfull")
except RuntimeError:
    pass

assert( not os.path.isfile(sqlite_test_filename1) and "sqlite file must not exist at this point" )
versioning_base.checkout("dbname=epanet_test_db",["epanet_trunk_rev_head.junctions","epanet_trunk_rev_head.pipes"], sqlite_test_filename1) 
assert( os.path.isfile(sqlite_test_filename1) and "sqlite file must exist at this point" )

try:
    versioning_base.checkout("dbname=epanet_test_db",["epanet_trunk_rev_head.junctions","epanet_trunk_rev_head.pipes"], sqlite_test_filename1) 
    assert(False and "trying to checkout on an existing file must fail")
except RuntimeError:
    pass

# edit one table and commit changes

scon = dbapi2.connect(sqlite_test_filename1)
scur = scon.cursor()
scur.execute("UPDATE junctions_view SET elevation = '8' WHERE id = '1'")
scon.commit()
scur.execute("SELECT COUNT(*) FROM junctions")
assert( scur.fetchone()[0] == 3 )
scon.close()
versioning_base.commit(sqlite_test_filename1, 'first test commit')
pcon = psycopg2.connect("dbname=epanet_test_db")
pcur = pcon.cursor()
pcur.execute("SELECT COUNT(*) FROM epanet.junctions")
assert( pcur.fetchone()[0] == 3 )
pcur.execute("SELECT COUNT(*) FROM epanet.revisions")
assert( pcur.fetchone()[0] == 2 )



