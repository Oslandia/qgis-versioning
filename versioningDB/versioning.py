# -*- coding: utf-8 -*-
""" This module provides functions to version a postgis DB and interact
with this DB. User can checkout a working copy, update and commit.
"""

from __future__ import absolute_import

import re
import os
from pyspatialite import dbapi2
import psycopg2
import platform
from . import utils
from .versioningAbc import versioningAbc

versioningDb = versioningAbc
def spatialite(sqlite_filename, pg_conn_info):
    return versioningDb([sqlite_filename, pg_conn_info], 'spatialite')

def pgServer(pg_conn_info, schema):
    return versioningDb([pg_conn_info, schema], 'postgres')

def pgLocal(pg_conn_info, schema, pg_conn_info_out):
    return versioningDb([pg_conn_info, schema, pg_conn_info_out], 'pgDistant')

Db = utils.Db
os_info = utils.os_info
pg_pk = utils.pg_pk
pg_geoms = utils.pg_geoms
pg_geom = utils.pg_geom
pg_branches = utils.pg_branches
pg_array_elem_type = utils.pg_array_elem_type
mem_field_names_types = utils.mem_field_names_types
get_pg_users_list = utils.get_pg_users_list
get_actual_pk = utils.get_actual_pk
preserve_fid = utils.preserve_fid
escape_quote = utils.escape_quote
quote_ident = utils.quote_ident
get_username = utils.get_username

DEBUG = False


gdal_mac_path = "/Library/Frameworks/GDAL.framework/Programs"
if any(platform.mac_ver()) and gdal_mac_path not in os.environ["PATH"]:
    os.environ["PATH"] += ":"+gdal_mac_path

def historize( pg_conn_info, schema ):
    """Create historisation for the given schema"""
    if not schema:
        raise RuntimeError("no schema specified")
    pcur = utils.Db(psycopg2.connect(pg_conn_info))

    pcur.execute("""
        CREATE TABLE {schema}.revisions (
            rev serial PRIMARY KEY,
            commit_msg varchar,
            branch varchar DEFAULT 'trunk',
            date timestamp DEFAULT current_timestamp,
            author varchar)
            """.format(schema=schema))
    pcur.commit()
    pcur.close()
    add_branch( pg_conn_info, schema, 'trunk', 'initial commit' )

def createIndex(pcur, schema, table, branch):
    """ create index on columns used for versinoning"""
    for ext in ["_rev_begin", "_rev_end", "_parent", "_child"]:
        query = "CREATE INDEX IF NOT EXISTS idx_rev_%s%s ON %s.%s (%s%s)"
        data = (table, ext, schema, table, branch, ext)
        pcur.execute(query % data)
            
def add_branch( pg_conn_info, schema, branch, commit_msg,
        base_branch='trunk', base_rev='head' ):
    """Create a new branch (add 4 columns to tables)"""
    pcur = utils.Db(psycopg2.connect(pg_conn_info))

    # check that branch doesn't exist and that base_branch exists
    # and that base_rev is ok
    pcur.execute("SELECT * FROM "+schema+".revisions "
        "WHERE branch = '"+branch+"'")
    if pcur.fetchone():
        pcur.close()
        raise RuntimeError("Branch "+branch+" already exists")
    pcur.execute("SELECT * FROM "+schema+".revisions "
        "WHERE branch = '"+base_branch+"'")
    if branch != 'trunk' and not pcur.fetchone():
        pcur.close()
        raise RuntimeError("Base branch "+base_branch+" doesn't exist")
    pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
    [max_rev] = pcur.fetchone()
    if not max_rev:
        max_rev = 0
    if base_rev != 'head' and (int(base_rev) > max_rev or int(base_rev) <= 0):
        pcur.close()
        raise RuntimeError("Revision "+str(base_rev)+" doesn't exist")
    if DEBUG: print ('max rev = ', max_rev)

    pcur.execute("INSERT INTO "+schema+".revisions(rev, branch, commit_msg ) "
        "VALUES ("+str(max_rev+1)+", '"+branch+"', '"+ utils.escape_quote(commit_msg)+"')")
    pcur.execute("CREATE SCHEMA "+schema+"_"+branch+"_rev_head")

    history_columns = sum([
        [brch+'_rev_end', brch+'_rev_begin',
        brch+'_child', brch+'_parent' ] for brch in  utils.pg_branches( pcur, schema )],[])

    security = ' WITH (security_barrier)'
    pcur.execute("SELECT version()")
    [version] = pcur.fetchone()
    mtch = re.match( r'^PostgreSQL (\d+)\.(\d+)\.(\d+) ', version )
    if mtch and int(mtch.group(1)) <= 9 and int(mtch.group(2)) <= 2 :
        security = ''

    # note: do not version views
    pcur.execute("SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = '"+schema+"' "
        "AND table_type = 'BASE TABLE'")
    for [table] in pcur.fetchall():
        if table == 'revisions':
            continue

        try:
            pkey =  utils.pg_pk( pcur, schema, table )
        except:
            if 'VERSIONING_NO_PK' in os.environ and os.environ['VERSIONING_NO_PK'] == 'skip':
                if DEBUG: print (schema+'.'+table+' has no primary key, skipping')
            else:
                raise RuntimeError(schema+'.'+table+' has no primary key')

        pcur.execute("ALTER TABLE "+schema+"."+table+" "
            "ADD COLUMN "+branch+"_rev_begin integer "
            "REFERENCES "+schema+".revisions(rev), "
            "ADD COLUMN "+branch+"_rev_end   integer "
            "REFERENCES "+schema+".revisions(rev), "
            "ADD COLUMN "+branch+"_parent    integer "
            "REFERENCES "+schema+"."+table+"("+pkey+"),"
            "ADD COLUMN "+branch+"_child     integer "
            "REFERENCES "+schema+"."+table+"("+pkey+")")
        createIndex(pcur, schema, table, branch)

        if branch == 'trunk': # initial versioning
            pcur.execute("UPDATE "+schema+"."+table+" "
                "SET "+branch+"_rev_begin = (SELECT MAX(rev) "
                                            "FROM "+schema+".revisions)")
        elif base_rev == "head":
            pcur.execute("UPDATE "+schema+"."+table+" "
                    "SET "+branch+"_rev_begin = (SELECT MAX(rev) "
                                                "FROM "+schema+".revisions "
                    "WHERE "+base_branch+"_rev_end IS NULL "
                    "AND "+base_branch+"_rev_begin IS NOT NULL)")
        else:
            pcur.execute("UPDATE "+schema+"."+table+" "
                    "SET "+branch+"_rev_begin = (SELECT MAX(rev) "
                                                "FROM "+schema+".revisions)"
                    "WHERE ("+base_branch+"_rev_end IS NULL "
                            "OR "+base_branch+"_rev_end > "+base_rev+") "
                    "AND "+base_branch+"_rev_begin IS NOT NULL")

        pcur.execute("SELECT column_name "
                "FROM information_schema.columns "
                "WHERE table_schema = '"+schema+"' "
                "AND table_name = '"+table+"'")
        cols = ""
        for [col] in pcur.fetchall():
            if col not in history_columns:
                cols =  utils.quote_ident(col)+", "+cols
        cols = cols[:-2] # remove last coma and space
        pcur.execute("CREATE VIEW "+schema+"_"+branch+"_rev_head."+table+" "
            +security+" AS "
            "SELECT "+cols+" FROM "+schema+"."+table+" "
            "WHERE "+branch+"_rev_end IS NULL "
            "AND "+branch+"_rev_begin IS NOT NULL")
    pcur.commit()
    pcur.close()

def diff_rev_view_str(pg_conn_info, schema, table, branch, rev_begin, rev_end):
    """DIFFerence_REVision_VIEW_STRing
    Create the SQL view string of the specified revision difference (comparison).
    """
    rev_begin = str(rev_begin)
    rev_end = str(rev_end)

    pcur = utils.Db(psycopg2.connect(pg_conn_info))

    pcur.execute("SELECT * FROM "+schema+".revisions "
        "WHERE branch = '"+branch+"'")
    if not pcur.fetchone():
        pcur.close()
        raise RuntimeError("Branch "+branch+" doesn't exist")
    pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
    [max_rev] = pcur.fetchone()
    if int(rev_begin) > max_rev or int(rev_begin) <= 0:
        pcur.close()
        raise RuntimeError("Revision 1 (begin) "+rev_begin+" doesn't exist")
    if int(rev_end) > max_rev or int(rev_end) <= 0:
        pcur.close()
        raise RuntimeError("Revision 2 (end) "+rev_end+" doesn't exist")

    select_str = ("SELECT "
    "CASE WHEN "
        +schema+"."+table+"."+branch+"_rev_begin > "+rev_begin+ " "
        "AND " +schema+"."+table+"."+branch+"_rev_begin <= "+rev_end+ " "
        "AND " +schema+"."+table+"."+branch+"_parent IS NULL THEN 'a' "
    "WHEN (" +schema+"."+table+"."+branch+"_rev_begin > "+rev_begin+ " "
        "AND " +schema+"."+table+"."+branch+"_rev_end IS NULL "
        "AND " +schema+"."+table+"."+branch+"_parent IS NOT NULL) "
        "OR ("+schema+"."+table+"."+branch+"_rev_end >= "+rev_end+" "
        "AND "+schema+"."+table+"."+branch+"_child IS NOT NULL) "
        "THEN 'u' "
    "WHEN " +schema+"."+table+"."+branch+"_rev_end > "+rev_begin+ " "
        "AND " +schema+"."+table+"."+branch+"_rev_end < "+rev_end+ " "
        "AND " +schema+"."+table+"."+branch+"_child IS NULL THEN 'd' ELSE 'i' END "
    "as diff_status, * FROM "+schema+"."+table+ " "
    "WHERE (" +schema+"."+table+"."+branch+"_rev_begin > "+rev_begin+ " "
        "AND " +schema+"."+table+"."+branch+"_rev_begin <= "+rev_end+") "
        "OR (" +schema+"."+table+"."+branch+"_rev_end > "+rev_begin+ " "
        "AND " +schema+"."+table+"."+branch+"_rev_end <= "+rev_end+ " )")

    pcur.close()
    return select_str

def rev_view_str(pg_conn_info, schema, table, branch, rev):
    """REVision_VIEW_STRing
    Create the SQL view string of the specified revision.
    Replaces add_revision_view()
    """
    pcur = utils.Db(psycopg2.connect(pg_conn_info))

    pcur.execute("SELECT * FROM "+schema+".revisions "
        "WHERE branch = '"+branch+"'")
    if not pcur.fetchone():
        pcur.close()
        raise RuntimeError("Branch "+branch+" doesn't exist")
    pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
    [max_rev] = pcur.fetchone()
    if int(rev) > max_rev or int(rev) <= 0:
        pcur.close()
        raise RuntimeError("Revision "+str(rev)+" doesn't exist")

    select_str = "SELECT * FROM "+schema+"."+table
    #print "select_str = " + select_str
    where_str = ("("+branch + "_rev_end IS NULL "
        "OR "+branch+"_rev_end >= "+str(rev) + ") "
         "AND "+branch+"_rev_begin <= "+str(rev) )

    pcur.close()
    return select_str, where_str

def add_revision_view(pg_conn_info, schema, branch, rev):
    """Create schema with views of the specified revision.
    Deprecated as of version 0.5.
    """
    pcur = utils.Db(psycopg2.connect(pg_conn_info))

    pcur.execute("SELECT * FROM "+schema+".revisions "
        "WHERE branch = '"+branch+"'")
    if not pcur.fetchone():
        pcur.close()
        raise RuntimeError("Branch "+branch+" doesn't exist")
    pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
    [max_rev] = pcur.fetchone()
    if int(rev) > max_rev or int(rev) <= 0:
        pcur.close()
        raise RuntimeError("Revision "+str(rev)+" doesn't exist")

    history_columns = sum([
        [brch+'_rev_end', brch+'_rev_begin',
        brch+'_child', brch+'_parent' ] for brch in  utils.pg_branches( pcur, schema )],[])

    rev_schema = schema+"_"+branch+"_rev_"+str(rev)

    pcur.execute("SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name = '"+rev_schema+"'")
    if pcur.fetchone():
        if DEBUG: print (rev_schema, ' already exists')
        return

    security = ' WITH (security_barrier)'
    pcur.execute("SELECT version()")
    [version] = pcur.fetchone()
    mtch = re.match( r'^PostgreSQL (\d+)\.(\d+)\.(\d+) ', version )
    if mtch and int(mtch.group(1)) <= 9 and int(mtch.group(2)) <= 2 :
        security = ''

    pcur.execute("CREATE SCHEMA "+rev_schema)

    pcur.execute("SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = '"+schema+"' "
        "AND table_type = 'BASE TABLE'")

    for [table] in pcur.fetchall():
        if table == 'revisions':
            continue
        pcur.execute("SELECT column_name "
                "FROM information_schema.columns "
                "WHERE table_schema = '"+schema+"' "
                "AND table_name = '"+table+"'")
        cols = ""
        for [col] in pcur.fetchall():
            if col not in history_columns:
                cols =  utils.quote_ident(col)+", "+cols
        cols = cols[:-2] # remove last coma and space
        pcur.execute("CREATE VIEW "+rev_schema+"."+table+" "+security+" AS "
           "SELECT "+cols+" FROM "+schema+"."+table+" "
           "WHERE ("+branch+"_rev_end IS NULL "
                   "OR "+branch+"_rev_end >= "+str(rev)+") "
           "AND "+branch+"_rev_begin <= "+str(rev))

    pcur.commit()
    pcur.close()

def revisions(pg_conn_info, schema):
    """returns a list of revisions for this schema"""
    pcur = utils.Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT rev FROM "+schema+".revisions")
    revs = []
    for [res] in pcur.fetchall():
        revs.append(res)
    pcur.close()
    return revs

def archive(pg_conn_info, schema, revision_end):
    """Archiving tables from schema ended at revision_end"""

    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = '"+schema+"' "
        "AND table_type = 'BASE TABLE'")

    for [table] in pcur.fetchall():
        if table == 'revisions': 
            continue
        found = table.rfind('_archive')
        if found != -1 and table[found:] == '_archive':
            continue
        
        pk = utils.pg_pk(pcur, schema, table)
        # get columns from table. ONLY revisionned table and 4 columns for revision can be used
        pcur.execute("""WITH pos as (
                    SELECT ordinal_position FROM information_schema.columns 
                    WHERE table_schema = '{schema}' AND table_name = '{table}' and column_name = 'trunk_child'
                    )
                    SELECT column_name FROM information_schema.columns WHERE
                    table_schema = '{schema}' AND table_name = '{table}' and ordinal_position <= (SELECT ordinal_position FROM pos)""".format(schema=schema, table=table))
        lcols = pcur.fetchall()
        cols = ', '.join(list(zip(*lcols)[0]))
        
        pcur.execute("""SELECT EXISTS
                     (SELECT 1 
                     FROM information_schema.tables
                     WHERE  table_schema = '{schema}' AND
                     table_name = '{table}_archive' )""".format(schema=schema, table=table))
        exists = pcur.fetchone()[0]
        if not exists:
            sql = """CREATE TABLE {schema}.{table}_archive as SELECT {cols} FROM {schema}.{table} LIMIT 0""".format(schema=schema, table=table, cols=cols)
            if DEBUG: 
                print(sql)
                
            pcur.execute(sql)
            
            pcur.execute("""ALTER TABLE {schema}.{table}_archive ADD PRIMARY KEY ({pk})""".format(schema=schema,
                        table=table, pk=pk))
            pcur.execute("""ALTER TABLE {schema}.{table}_archive ADD COLUMN date_archiving timestamp without time zone DEFAULT now()""".format(schema=schema,
                        table=table))
            createIndex(pcur, schema, table, 'trunk')
        
        pcur.execute("""INSERT INTO {schema}.{table}_archive ({cols}) (SELECT {cols} 
                    FROM {schema}.{table} 
                    WHERE trunk_rev_end <= {rev_number})""".format(schema=schema,
                    table=table, rev_number=revision_end, cols=cols))
        
        pcur.execute("""UPDATE {schema}.{table} 
                    SET trunk_parent = NULL 
                    WHERE {pk} IN (
                    SELECT trunk_child 
                    FROM {schema}.{table} 
                    WHERE trunk_rev_end <= {rev_number})""".format(
                    schema=schema,
                    table=table, 
                    rev_number=revision_end,
                    pk=pk))
        pcur.execute("""DELETE FROM {schema}.{table} WHERE trunk_rev_end <= {rev_number}""".format(schema=schema,
                    table=table, rev_number=revision_end))
    pcur.commit()
    pcur.close()
        