#!/usr/bin/python
import versioning_base
from pyspatialite import dbapi2
import psycopg2
import os
import shutil

test_data_dir = os.path.dirname(os.path.realpath(__file__))
tmp_dir = "/tmp"

# create the test database

os.system("dropdb big_epanet_test_db")
os.system("createdb big_epanet_test_db")
os.system("psql big_epanet_test_db -f "+test_data_dir+"/big_epanet_test_db.sql")

pg_conn_info = "dbname=big_epanet_test_db"

versioning_base.historize( pg_conn_info, 'epanet' )

failed = False
try:
    versioning_base.add_branch( pg_conn_info, 'epanet', 'trunk' )
except: 
    failed = True
assert( failed )

failed = False
try:
    versioning_base.add_branch( pg_conn_info, 'epanet', 'mybranch', 'message', 'toto' )
except:
    failed = True
assert( failed )

versioning_base.add_branch( pg_conn_info, 'epanet', 'mybranch', 'test msg' )


pcur = versioning_base.Db(psycopg2.connect(pg_conn_info))
pcur.execute("SELECT * FROM epanet_mybranch_rev_head.junctions")
assert( len(pcur.fetchall()) == 321 )
pcur.execute("SELECT * FROM epanet_mybranch_rev_head.pipes")
assert( len(pcur.fetchall()) == 370 )

versioning_base.add_revision_view( pg_conn_info, 'epanet', 'mybranch', 2)
pcur.execute("SELECT * FROM epanet_mybranch_rev_2.junctions")
assert( len(pcur.fetchall()) == 321 )
pcur.execute("SELECT * FROM epanet_mybranch_rev_2.pipes")
assert( len(pcur.fetchall()) == 370 )

pcur.close()

tables = [
    'epanet_trunk_rev_head.curves',
    'epanet_trunk_rev_head.demands',
    'epanet_trunk_rev_head.junctions',
    'epanet_trunk_rev_head.energy',
    'epanet_trunk_rev_head.options',
    'epanet_trunk_rev_head.patterns',
    'epanet_trunk_rev_head.pipes',
    'epanet_trunk_rev_head.pumps',
    'epanet_trunk_rev_head.quality',
    'epanet_trunk_rev_head.report',
    'epanet_trunk_rev_head.reservoirs',
    'epanet_trunk_rev_head.rules',
    'epanet_trunk_rev_head.tanks',
    'epanet_trunk_rev_head.times',
    'epanet_trunk_rev_head.valves']

f = tmp_dir+'/big_epanet_test_db.sqlite'
if os.path.isfile(f): os.remove(f) 
versioning_base.checkout(pg_conn_info, tables, f)

scur = versioning_base.Db( dbapi2.connect( f ) )

scur.execute("UPDATE junctions_view SET GEOMETRY = ShiftCoords(GEOMETRY,1000,1000) WHERE OGC_FID = 1")
scur.commit()

versioning_base.commit( f, "commit 1", pg_conn_info )


