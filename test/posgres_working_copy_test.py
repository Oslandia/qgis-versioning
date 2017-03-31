#!/usr/bin/python
from .. import versioning
import psycopg2
import os
import shutil

def prtTab( cur, tab ):
    print "--- ",tab," ---"
    pcur.execute("SELECT pid, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child, length FROM "+tab)
    for r in pcur.fetchall():
        t = []
        for i in r: t.append(str(i))
        print '\t| '.join(t)

def prtHid( cur, tab ):
    print "--- ",tab," ---"
    pcur.execute("SELECT pid FROM "+tab)
    for [r] in pcur.fetchall(): print r

if __name__ == "__main__":
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    # create the test database

    os.system("dropdb epanet_test_db")
    os.system("createdb epanet_test_db")
    os.system("psql epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")

    # chechout
    #tables = ['epanet_trunk_rev_head.junctions','epanet_trunk_rev_head.pipes']
    tables = ['epanet_trunk_rev_head.junctions', 'epanet_trunk_rev_head.pipes']
    versioning.pg_checkout("dbname=epanet_test_db",tables, "epanet_working_copy")

    versioning.pg_checkout("dbname=epanet_test_db",tables, "epanet_working_copy_cflt")

    pcur = versioning.Db(psycopg2.connect("dbname=epanet_test_db"))


    pcur.execute("INSERT INTO epanet_working_copy.pipes_view(id, start_node, end_node, geom) VALUES ('2','1','2',ST_GeometryFromText('LINESTRING(1 1,0 1)',2154))")
    pcur.execute("INSERT INTO epanet_working_copy.pipes_view(id, start_node, end_node, geom) VALUES ('3','1','2',ST_GeometryFromText('LINESTRING(1 -1,0 1)',2154))")
    pcur.commit()


    prtHid(pcur, 'epanet_working_copy.pipes_view')

    pcur.execute("SELECT pid FROM epanet_working_copy.pipes_view")
    assert( len(pcur.fetchall()) == 3 )
    pcur.execute("SELECT pid FROM epanet_working_copy.pipes_diff")
    assert( len(pcur.fetchall()) == 2 )
    pcur.execute("SELECT pid FROM epanet.pipes")
    assert( len(pcur.fetchall()) == 1 )


    prtTab(pcur, 'epanet.pipes')
    prtTab(pcur, 'epanet_working_copy.pipes_diff')
    pcur.execute("UPDATE epanet_working_copy.pipes_view SET length = 4 WHERE pid = 1")
    prtTab(pcur, 'epanet_working_copy.pipes_diff')
    pcur.execute("UPDATE epanet_working_copy.pipes_view SET length = 5 WHERE pid = 4")
    prtTab(pcur, 'epanet_working_copy.pipes_diff')

    pcur.execute("DELETE FROM epanet_working_copy.pipes_view WHERE pid = 4")
    prtTab(pcur, 'epanet_working_copy.pipes_diff')
    pcur.commit()

    versioning.pg_commit("dbname=epanet_test_db", "epanet_working_copy","test commit msg")
    prtTab(pcur, 'epanet.pipes')

    pcur.execute("SELECT trunk_rev_end FROM epanet.pipes WHERE pid = 1")
    assert( 1 == pcur.fetchone()[0] )
    pcur.execute("SELECT COUNT(*) FROM epanet.pipes WHERE trunk_rev_begin = 2")
    assert( 2 == pcur.fetchone()[0] )


    # modify the second working copy to create conflict
    prtTab(pcur, 'epanet.pipes')
    pcur.execute("SELECT * FROM epanet_working_copy_cflt.initial_revision")
    print '-- epanet_working_copy_cflt.initial_revision ---'
    for r in pcur.fetchall(): print r

    prtHid(pcur, 'epanet_working_copy_cflt.pipes_view')
    prtTab(pcur, 'epanet_working_copy_cflt.pipes_diff')
    pcur.execute("UPDATE epanet_working_copy_cflt.pipes_view SET length = 8 WHERE pid = 1")
    pcur.commit()
    prtTab(pcur, 'epanet.pipes')
    prtTab(pcur, 'epanet_working_copy_cflt.pipes_diff')
    pcur.execute("SELECT COUNT(*) FROM epanet_working_copy_cflt.pipes_diff")
    for l in pcur.con.notices: print l
    assert( 2 == pcur.fetchone()[0] )


    pcur.execute("INSERT INTO epanet_working_copy_cflt.pipes_view(id, start_node, end_node, geom) VALUES ('3','1','2',ST_GeometryFromText('LINESTRING(1 -1,0 1)',2154))")
    prtTab(pcur, 'epanet_working_copy_cflt.pipes_diff')
    pcur.commit()
    versioning.pg_update( "dbname=epanet_test_db", "epanet_working_copy_cflt" )
    prtTab(pcur, 'epanet_working_copy_cflt.pipes_diff')
    prtTab(pcur, 'epanet_working_copy_cflt.pipes_update_diff')

    pcur.execute("SELECT COUNT(*) FROM epanet_working_copy_cflt.pipes_conflicts")
    assert( 2 == pcur.fetchone()[0] )
    pcur.execute("SELECT COUNT(*) FROM epanet_working_copy_cflt.pipes_conflicts WHERE origin = 'mine'")
    assert( 1 == pcur.fetchone()[0] )
    pcur.execute("SELECT COUNT(*) FROM epanet_working_copy_cflt.pipes_conflicts WHERE origin = 'theirs'")
    assert( 1 == pcur.fetchone()[0] )

    prtTab(pcur, 'epanet_working_copy_cflt.pipes_conflicts')

    pcur.execute("DELETE FROM epanet_working_copy_cflt.pipes_conflicts WHERE origin = 'theirs'")
    pcur.execute("SELECT COUNT(*) FROM epanet_working_copy_cflt.pipes_conflicts")
    assert( 0 == pcur.fetchone()[0] )
    prtTab(pcur, 'epanet_working_copy_cflt.pipes_diff')
    prtTab(pcur, 'epanet_working_copy_cflt.pipes_conflicts')
    pcur.commit()

    versioning.pg_commit("dbname=epanet_test_db", "epanet_working_copy_cflt","second test commit msg")


    pcur.execute("SELECT * FROM epanet_working_copy_cflt.initial_revision")
    print '-- epanet_working_copy_cflt.initial_revision ---'
    for r in pcur.fetchall(): print r

    prtHid(pcur, 'epanet_working_copy_cflt.pipes_view')
    prtTab(pcur, 'epanet_working_copy_cflt.pipes_diff')

    pcur.execute("UPDATE epanet_working_copy_cflt.pipes_view SET length = 8")
    prtTab(pcur, 'epanet_working_copy_cflt.pipes_diff')
    pcur.commit()

    versioning.pg_commit("dbname=epanet_test_db", "epanet_working_copy_cflt","third test commit msg")


    prtTab(pcur, 'epanet_working_copy_cflt.pipes_diff')
    pcur.execute("UPDATE epanet_working_copy_cflt.pipes_view SET length = 12")
    pcur.commit()

