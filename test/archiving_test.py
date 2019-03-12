# -*- coding: utf-8 -*-
#!/usr/bin/env python3
from __future__ import absolute_import
import sys
sys.path.insert(0, '..')

from versioningDB import versioning
import psycopg2
import os

def printTab(pcur, schema, table):
    pk = 'pid'
    try:
        pk = versioning.pg_pk(pcur, schema, table)
    except:
        pass
    
    print("\n**********************************")
    print(schema+"."+table)
    pcur.execute("""SELECT column_name FROM information_schema.columns WHERE
                    table_schema = '{schema}' AND table_name = '{table}'""".format(schema=schema,
                    table=table))
    cols = ",".join([i[0] for i in pcur.fetchall()])
    print(cols)
    
    pcur.execute("""SELECT * FROM {schema}.{table} ORDER BY {pk}""".format(schema=schema, table=table, pk=pk))
    
    rows = pcur.fetchall()
    for row in rows:
        r = ', '.join([str(l) for l in list(row)])
        print(r)
    print("**********************************\n")
    
def test(host, pguser):
    pg_conn_info = "dbname=epanet_test_db host=" + host + " user=" + pguser
    test_data_dir = os.path.dirname(os.path.realpath(__file__))

    # create the test database

    os.system("dropdb --if-exists -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("createdb -h " + host + " -U "+pguser+" epanet_test_db")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql -h " + host + " -U "+pguser+" epanet_test_db -f "+test_data_dir+"/epanet_test_db.sql")

    # chechout
    #tables = ['epanet_trunk_rev_head.junctions','epanet_trunk_rev_head.pipes']
    tables = ['epanet_trunk_rev_head.junctions', 'epanet_trunk_rev_head.pipes']
    pgversioning = versioning.pgServer(pg_conn_info, 'epanet_working_copy')
    pgversioning.checkout(tables)

    pcur = versioning.Db(psycopg2.connect(pg_conn_info))


    pcur.execute("INSERT INTO epanet_working_copy.pipes_view(id, start_node, end_node, geom) VALUES ('2','1','2',ST_GeometryFromText('LINESTRING(1 1,0 1)',2154))")
    pcur.commit()
    pgversioning.commit("rev 1")
    pcur.execute("INSERT INTO epanet_working_copy.pipes_view(id, start_node, end_node, geom) VALUES ('3','1','2',ST_GeometryFromText('LINESTRING(1 -1,0 1)',2154))")
    pcur.commit()
    pgversioning.commit("rev 2")
    pcur.execute("INSERT INTO epanet_working_copy.pipes_view(id, start_node, end_node, geom) VALUES ('4','1','2',ST_GeometryFromText('LINESTRING(1 -1,0 1)',2154))")
    pcur.commit()
    pgversioning.commit("rev 3")
    pcur.execute("INSERT INTO epanet_working_copy.pipes_view(id, start_node, end_node, geom) VALUES ('5','1','2',ST_GeometryFromText('LINESTRING(1 -1,0 1)',2154))")
    pcur.commit()
    pgversioning.commit("rev 4")
    pcur.execute("DELETE FROM epanet_working_copy.pipes_view S WHERE pid = 5")
    pcur.commit()
    pgversioning.commit("rev 5")
    pcur.execute("INSERT INTO epanet_working_copy.pipes_view(id, start_node, end_node, geom) VALUES ('6','1','2',ST_GeometryFromText('LINESTRING(1 -1,0 1)',2154))")
    pcur.commit()
    pgversioning.commit("rev 6")
    pcur.execute("UPDATE epanet_working_copy.pipes_view SET length = 4 WHERE pid = 3")
    pcur.commit()
    pgversioning.commit("rev 7")
    pcur.execute("UPDATE epanet_working_copy.pipes_view SET length = 4 WHERE pid = 1")
    pcur.commit()
    pgversioning.commit("rev 8")
    pcur.execute("INSERT INTO epanet_working_copy.pipes_view(id, start_node, end_node, geom) VALUES ('7','1','2',ST_GeometryFromText('LINESTRING(1 -1,0 1)',2154))")
    pcur.commit()
    pgversioning.commit("rev 9")
    pcur.execute("INSERT INTO epanet_working_copy.pipes_view(id, start_node, end_node, geom) VALUES ('8','1','2',ST_GeometryFromText('LINESTRING(1 -1,0 1)',2154))")
    pcur.commit()
    pgversioning.commit("rev 10")
    pcur.execute("DELETE FROM epanet_working_copy.pipes_view S WHERE pid = 7")
    pcur.commit()
    pgversioning.commit("rev 11")
    pcur.execute("INSERT INTO epanet_working_copy.pipes_view(id, start_node, end_node, geom) VALUES ('9','1','2',ST_GeometryFromText('LINESTRING(1 -1,0 1)',2154))")
    pcur.commit()
    pgversioning.commit("rev 12")
    
    pcur.execute("SELECT * FROM epanet.pipes ORDER BY pid")
    end = pcur.fetchall()
    
    printTab(pcur, 'epanet', 'pipes')
    pcur.execute("SELECT count(*) FROM epanet.pipes")
    [ret] = pcur.fetchone()
    assert(ret == 11)
    
    versioning.archive(pg_conn_info, 'epanet', 7)
    printTab(pcur, 'epanet', 'pipes')
    pcur.execute("SELECT count(*) FROM epanet.pipes")
    [ret] = pcur.fetchone()
    assert(ret == 9)
    pcur.execute("SELECT pid FROM epanet.pipes ORDER BY pid")
    assert([i[0] for i in pcur.fetchall()] == [1, 2, 4, 6, 7, 8, 9, 10, 11])
    printTab(pcur, 'epanet_archive', 'pipes')
    pcur.execute("SELECT count(*) FROM epanet_archive.pipes")
    [ret] = pcur.fetchone()
    assert(ret == 2)
    pcur.execute("SELECT pid FROM epanet_archive.pipes ORDER BY pid")
    assert([i[0] for i in pcur.fetchall()] == [3, 5])
    
    versioning.archive(pg_conn_info, 'epanet', 11)
    printTab(pcur, 'epanet', 'pipes')
    pcur.execute("SELECT count(*) FROM epanet.pipes")
    [ret] = pcur.fetchone()
    assert(ret == 7)
    pcur.execute("SELECT pid FROM epanet.pipes ORDER BY pid")
    assert([i[0] for i in pcur.fetchall()] == [2, 4, 6, 8, 9, 10, 11])
    printTab(pcur, 'epanet_archive', 'pipes')
    pcur.execute("SELECT count(*) FROM epanet_archive.pipes")
    [ret] = pcur.fetchone()
    assert(ret == 4)
    pcur.execute("SELECT pid FROM epanet_archive.pipes ORDER BY pid")
    assert([i[0] for i in pcur.fetchall()] == [1, 3, 5, 7])
    
    # view
    printTab(pcur, 'epanet_archive', 'pipes_all')
    pcur.execute("SELECT count(*) FROM epanet_archive.pipes_all")
    [ret] = pcur.fetchone()
    assert(ret == 11)
    pcur.execute("SELECT * FROM epanet_archive.pipes_all ORDER BY pid")
    endv = pcur.fetchall()
    assert(end==endv)
    
    pcur.close()
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 archiving_test.py host pguser")
    else:
        test(*sys.argv[1:])
