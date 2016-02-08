#!/usr/bin/python
import versioning_base
from pyspatialite import dbapi2
import psycopg2
import os

tmp_dir = "/tmp"
test_data_dir = os.path.dirname(os.path.realpath(__file__))

sqlite_test_filename1 = tmp_dir+"/versioning_base_test1.sqlite"
sqlite_test_filename2 = tmp_dir+"/versioning_base_test2.sqlite"
sqlite_test_filename3 = tmp_dir+"/versioning_base_test3.sqlite"
sqlite_test_filename4 = tmp_dir+"/versioning_base_test4.sqlite"
if os.path.isfile(sqlite_test_filename1): os.remove(sqlite_test_filename1)
if os.path.isfile(sqlite_test_filename2): os.remove(sqlite_test_filename2)
if os.path.isfile(sqlite_test_filename3): os.remove(sqlite_test_filename3)
if os.path.isfile(sqlite_test_filename4): os.remove(sqlite_test_filename4)

# create the test database

os.system("dropdb epanet_test_db")
os.system("createdb epanet_test_db")
os.system("psql epanet_test_db -c 'CREATE EXTENSION postgis'")
os.system("psql epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")

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

# edit one table and commit changes; rev = 2

scon = dbapi2.connect(sqlite_test_filename1)
scur = scon.cursor()
scur.execute("UPDATE junctions_view SET elevation = '8' WHERE id = '1'")
scon.commit()
scur.execute("SELECT COUNT(*) FROM junctions")
assert( scur.fetchone()[0] == 3 )
scon.close()
versioning_base.commit(sqlite_test_filename1, 'first edit commit', "dbname=epanet_test_db")
pcon = psycopg2.connect("dbname=epanet_test_db")
pcur = pcon.cursor()
pcur.execute("SELECT COUNT(*) FROM epanet.junctions")
assert( pcur.fetchone()[0] == 3 )
pcur.execute("SELECT COUNT(*) FROM epanet.revisions")
assert( pcur.fetchone()[0] == 2 )

# add revision : edit one table and commit changes; rev = 3

versioning_base.checkout("dbname=epanet_test_db",["epanet_trunk_rev_head.junctions"], sqlite_test_filename2)

scon = dbapi2.connect(sqlite_test_filename2)
scur = scon.cursor()
scur.execute("UPDATE junctions_view SET elevation = '22' WHERE id = '1'")
scon.commit()
#scur.execute("SELECT COUNT(*) FROM junctions")
#assert( scur.fetchone()[0] == 3 )
scon.close()
versioning_base.commit(sqlite_test_filename2, 'second edit commit', "dbname=epanet_test_db")

# add revision : insert one junction and commit changes; rev = 4

versioning_base.checkout("dbname=epanet_test_db",["epanet_trunk_rev_head.junctions"], sqlite_test_filename3)

scon = dbapi2.connect(sqlite_test_filename3)
scur = scon.cursor()
scur.execute("INSERT INTO junctions_view(id, elevation, GEOMETRY) VALUES ('10','100',GeomFromText('POINT(2 0)',2154))")
scon.commit()
#scur.execute("SELECT COUNT(*) FROM junctions")
#assert( scur.fetchone()[0] == 3 )
scon.close()
versioning_base.commit(sqlite_test_filename3, 'insert commit', "dbname=epanet_test_db")

# add revision : delete one junction and commit changes; rev = 5

versioning_base.checkout("dbname=epanet_test_db",["epanet_trunk_rev_head.junctions"], sqlite_test_filename4)

scon = dbapi2.connect(sqlite_test_filename4)
scur = scon.cursor()
scur.execute("DELETE FROM junctions_view  WHERE id = 0")
scon.commit()
#scur.execute("SELECT COUNT(*) FROM junctions")
#assert( scur.fetchone()[0] == 3 )
scon.close()
versioning_base.commit(sqlite_test_filename4, 'delete id=0 commit', "dbname=epanet_test_db")

pg_conn_info = "dbname=epanet_test_db"
select_str = versioning_base.diff_rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1,2)
#print "select_str = " + select_str
pcur.execute(select_str)
res = pcur.fetchall()
print "fetchall 1 vs 2 = " + str(res)

select_and_where_str =  versioning_base.rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1)
select_str = select_and_where_str[0]
where_str = select_and_where_str[1]

select_str = versioning_base.diff_rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1,3)
#print "select_str = " + select_str
pcur.execute(select_str)
res = pcur.fetchall()
print "fetchall 1 vs 3 = " + str(res)

select_str = versioning_base.diff_rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1,4)
print "select_str 1 vs 4 = " + select_str
pcur.execute(select_str)
res = pcur.fetchall()
print "fetchall 1 vs 4 = " + str(res)

select_str = versioning_base.diff_rev_view_str(pg_conn_info, 'epanet', 'junctions','trunk', 1,5)
print "select_str 1 vs 5 = " + select_str
pcur.execute(select_str)
res = pcur.fetchall()
print "fetchall 1 vs 5 = " + str(res)
