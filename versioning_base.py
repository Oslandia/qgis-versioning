# -*- coding: utf-8 -*-
""" This module provides functions to version a postgis DB and interact
with this DB. User can checkout a working copy, update and commit.
"""
import re
import os
import getpass
from pyspatialite import dbapi2
import psycopg2
import codecs
from itertools import izip_longest
import platform, sys
import traceback

# Deactivate stdout (like output of print statements) because windows
# causes occasional "IOError [Errno 9] File descriptor error"
# Not needed when there is a way to run QGIS in console mode in Windows.
iswin = any(platform.win32_ver())
if iswin:
    sys.stdout = open(os.devnull, 'w')

def os_info():
    os_type = platform.system()
    if os_type == "Linux":
        os_info = platform.uname()[0]
    elif os_type == "Windows":
        os_info = "Windows"+platform.win32_ver()[0]
    elif os_type == "Darwin":
        os_info = "MacOS"+platform.mac_ver()[0]
    else:
        os_info = "UnknownOS"
    return os_info

def mem_field_names_types(pg_layer):
    '''String massaging to get field names and types in memory layer uri
    format.  The provider supports string, int and double fields.  Types
    returned by pg_layer need to be cast as follows :
    int4    => integer
    float8  => double
    varchar => string
    text => string
    Intended use in : versioning.mem_layer_uri
    To do : check for field type not supported and exit
    '''
    name_type_lst = [(str(f.name()), ':', str(f.typeName())) for f
        in pg_layer.pendingFields().toList()]
    field_list = [''.join(tuples) for tuples in name_type_lst]
    concatenated_field_str = ''
    for i in range(len(field_list)):
        concatenated_field_str += "field=" + field_list[i] +'&'

    #print "concatenated_field_str = " + concatenated_field_str +"\n"

    rep = {'float8': 'double', 'int4': 'integer', 'text': 'string',
        'varchar': 'string'}
    pattern = re.compile("|".join(rep.keys()))
    temp_str = pattern.sub(
        lambda m: rep[re.escape(m.group(0))],concatenated_field_str )
    final_str = temp_str[:-1]
    #print "\n" + "final_str = " + final_str +"\n"
    return final_str

def get_pg_users_list(pg_conn_info):
    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("select usename from pg_user order by usename ASC")
    pg_users_list = pcur.fetchall()
    pg_users_str_list=[]
    for i in pg_users_list:
        pg_users_str_list.append(str(i[0]))
    pcur.close()
    return pg_users_str_list

def get_actual_pk(uri,pg_conn_info):
    """Get actual PK from corresponding table or view.  The result serves to
    ascertain that the PK found by QGIS for PG views matches the real PK.
    """
    mtch = re.match(r'(.+)_([^_]+)_rev_(head|\d+)', uri.schema())
    pcur = Db(psycopg2.connect(pg_conn_info))
    actual_pk=pg_pk(pcur,mtch.group(1), uri.table())
    pcur.close()
    return actual_pk

def preserve_fid( pkid, fetchall_tuple):
    # This is a hack because os.system does not scale in MS Windows.
    # We need to create a view, then emulate the "preserve_fid" behaviour of
    # ogr2ogr.  A select * in the new view will generate random OGC_FID values
    # which means we cannot commit modifications after a checkout.
    # pkid = name of pkid as a string
    # fetchall_tuple = a list of column names returned as tuples by fetchall()

    str_list=[]
    for i in fetchall_tuple:
        str_list.append(str(i[0]))

    replaceText = pkid
    replaceData = pkid + ' as OGC_FID'
    pos = str_list.index(replaceText)
    str_list[pos] = replaceData
    columns_str = ', '.join(str_list)
    return columns_str

def escape_quote(msg):
    """quote single quotes"""
    return str.replace(str(msg),"'","''");

def quote_ident(ident):
    """Add quotes around identifier if it contains spaces"""
    if ident.find(' '):
        return '"'+ident+'"'
    else:
        return ident

class Db:
    """Basic wrapper arround DB cursor that allows for logging SQL commands"""
    def __init__(self, con, filename = ''):
        """The passed connection must be closed with close()"""
        self.con = con
        if isinstance(con, dbapi2.Connection):
            self.db_type = 'sp : '
        else :
            self.db_type = 'pg : '
        self.cur = self.con.cursor()
        if filename :
            self.log = codecs.open( filename, 'w', 'utf-8' )
            self.log.write('-- opening connection\n')
        else :
            self.log = None
        self.begun = False
        self._verbose = False

    def hasrow(self):
        """Test if previous execute returned rows"""
        if self._verbose:
            print self.db_type, self.cur.rowcount, ' rows returned'
        return self.cur.rowcount > 0

    def verbose(self, verbose):
        """Set verbose level"""
        self._verbose = verbose

    def execute(self, sql):
        """Execute SQL command"""
        if not self.begun:
            self.begun = True
            if self._verbose:
                print self.db_type, 'BEGIN;'
            if self.log :
                self.log.write( 'BEGIN;\n')
        if self._verbose:
            print self.db_type, sql, ';'
        if self.log :
            self.log.write(sql+';\n')
        try:
            self.cur.execute( sql )
        except Exception as e:
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("\n sql: {}\n\n".format(sql))
            raise e

            

    def fetchall(self):
        """Returns the result of the previous execute as a list of tuples"""
        return self.cur.fetchall()

    def fetchone(self):
        """Returns one row of result of the previous execute as a tuple"""
        return self.cur.fetchone()

    def commit(self):
        """Commit previous SQL command to DB, not necessary for SELECT"""
        if self._verbose:
            print self.db_type, 'END;'
        if self.log :
            self.log.write('END;\n')
        self.begun = False
        self.con.commit()

    def close(self):
        """Close DB connection"""
        if self.begun :
            if self._verbose:
                print self.db_type, 'END;'
            if self.log :
                self.log.write('END;\n')
        if self.log :
            self.log.write('-- closing connection\n')
        self.con.close()

def get_username():
    """Returns user name"""
    return getpass.getuser()

def pg_pk( cur, schema_name, table_name ):
    """Fetch the primary key of the specified postgis table"""
    cur.execute("SELECT quote_ident(a.attname) as column_name "
        "FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid "
        "AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = '\""+schema_name+'"."'+table_name+"\"'::regclass "
        "AND i.indisprimary")
    if not cur.hasrow():
        raise RuntimeError("table "+schema_name+"."+table_name+
                " does not have a primary key")
    [pkey] = cur.fetchone()
    return pkey

def pg_array_elem_type( cur, schema, table, column ):
    """Fetch type of elements of a column of type ARRAY"""
    cur.execute("SELECT e.data_type FROM information_schema.columns c "
        "LEFT JOIN information_schema.element_types e "
        "ON ((c.table_catalog, c.table_schema, c.table_name, "
            "'TABLE', c.dtd_identifier) "
        "= (e.object_catalog, e.object_schema, e.object_name, "
            "e.object_type, e.collection_type_identifier)) "
        "WHERE c.table_schema = '"+schema+"' "
        "AND c.table_name = '"+table+"' "
        "AND c.column_name = '"+column+"'")
    if not cur.hasrow():
        raise RuntimeError('column '+column+' of '
                +schema+'.'+table+' is not an ARRAY')
    [res] = cur.fetchone()
    return res

def pg_geoms( cur, schema_name, table_name ):
    """Fetch the list of geometry columns of the specified postgis table, empty if none"""
    cur.execute("SELECT f_geometry_column FROM geometry_columns "
        "WHERE f_table_schema = '"+schema_name+"' "
        "AND f_table_name = '"+table_name+"'")
    return [ geo[0] for geo in cur.fetchall() ]

def pg_geom( cur, schema_name, table_name ):
    """Fetch the first geometry column of the specified postgis table, empty string if none"""
    geoms = pg_geoms( cur, schema_name, table_name )
    if not geoms:
        return ''
    elif len(geoms) == 1:
        return geoms[0]
    elif 'VERSIONING_GEOMETRY_COLUMN' in os.environ:
        if os.environ['VERSIONING_GEOMETRY_COLUMN'] in geoms:
            return os.environ['VERSIONING_GEOMETRY_COLUMN']
        else:
            raise RuntimeError('more than one geometry column in '
                +schema_name+'.'+table_name+' but none is '
                +os.environ['VERSIONING_GEOMETRY_COLUMN']+
                ' (i.e. the value of VERSIONING_GEOMETRY_COLUMN) ')
    elif 'geometry' in geoms:
        return 'geometry'
    else:
        raise RuntimeError('more than one geometry column in '
            +schema_name+'.'+table_name+
            ' but the environment variable VERSIONING_GEOMETRY_COLUMN '
            'is not defined and the geometry column name is not geometry')

def unresolved_conflicts(sqlite_filename):
    """return a list of tables with unresolved conflicts"""
    found = []
    scur = Db(dbapi2.connect(sqlite_filename))
    scur.execute("SELECT tbl_name FROM sqlite_master "
        "WHERE type='table' AND tbl_name LIKE '%_conflicts'")
    for table_conflicts in scur.fetchall():
        print 'table_conflicts:', table_conflicts[0]
        scur.execute("SELECT * FROM "+table_conflicts[0])
        if scur.fetchone():
            found.append( table_conflicts[0][:-10] )
    scur.commit()
    scur.close()
    return found

def checkout(pg_conn_info, pg_table_names, sqlite_filename, selected_feature_lists = []):
    """create working copy from versioned database tables
    pg_table_names must be complete schema.table names
    the schema name must end with _branch_rev_head
    the file sqlite_filename must not exists
    the views and trigger for local edition will be created
    along with the tables and triggers for conflict resolution"""

    if os.path.isfile(sqlite_filename):
        raise RuntimeError("File "+sqlite_filename+" already exists")
    for pg_table_name in pg_table_names:
        [schema, table] = pg_table_name.split('.')
        if not ( schema and table and schema[-9:] == "_rev_head"):
            raise RuntimeError("Schema names must end with "
                "suffix _branch_rev_head")

    pcur = Db(psycopg2.connect(pg_conn_info))

    temp_view_names = []
    first_table = True
    for pg_table_name,feature_list in list(izip_longest(pg_table_names, selected_feature_lists)):
        [schema, table] = pg_table_name.split('.')
        [schema, sep, branch] = schema[:-9].rpartition('_')
        del sep

        # fetch the current rev
        pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
        current_rev = int(pcur.fetchone()[0])

        # max pkey for this table
        pkey = pg_pk( pcur, schema, table )
        pcur.execute("SELECT MAX("+pkey+") FROM "+schema+"."+table)
        [max_pg_pk] = pcur.fetchone()
        if not max_pg_pk :
            max_pg_pk = 0

        # use ogr2ogr to create spatialite db
        if first_table:
            first_table = False
            cmd = ['ogr2ogr',
                    '-preserve_fid',
                    '-f', 'SQLite',
                    '-dsco', 'SPATIALITE=yes',
                    '"' + sqlite_filename + '"',
                    'PG:"'+pg_conn_info+'"', schema+'.'+table,
                    '-nln', table]
            # We need to create a temp view because of windows commandline
            # limitations, e.g. ogr2ogr with a very long where clause
            # GDAL > 2.1 allows specifying a filename for where args, e.g.
            #cmd += ['-where', '"'+pkey+' in ('+",".join([str(feature_list[i]) for i in range(0, len(feature_list))])+')"']
            temp_view_name = schema+"."+table+"_checkout_temp_view"
            temp_view_names.append(temp_view_name)
            # Get column names because we cannot just call 'SELECT *'
            pcur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = \'"+schema+"\' AND table_name   = \'"+table+"\'")
            column_list = pcur.fetchall()
            new_columns_str = preserve_fid( pkey, column_list)
            view_str = "CREATE OR REPLACE VIEW "+temp_view_name+" AS SELECT "+new_columns_str+" FROM " +schema+"."+table
            if feature_list:
                view_str = "CREATE OR REPLACE VIEW "+temp_view_name+" AS SELECT "+new_columns_str+" FROM " +schema+"."+table+" WHERE "+pkey+' in ('+",".join([str(feature_list[i]) for i in range(0, len(feature_list))])+')'
            pcur.execute(view_str)
            pcur.commit()
            cmd[8] = temp_view_name

            os.system(' '.join(cmd))

            # save target revision in a table
            scur = Db(dbapi2.connect(sqlite_filename))
            scur.execute("CREATE TABLE initial_revision AS SELECT "+
                    str(current_rev)+" AS rev, '"+
                    branch+"' AS branch, '"+
                    schema+"' AS table_schema, '"+
                    table+"' AS table_name, "+
                    str(max_pg_pk)+" AS max_pk")
            scur.commit()
            scur.close()

        else:
            cmd = ['ogr2ogr',
                        '-preserve_fid',
                        '-f', 'SQLite',
                        '-update',
                        '"' + sqlite_filename + '"',
                        'PG:"'+pg_conn_info+'"', schema+'.'+table,
                        '-nln', table]
            # Same comments as in 'if feature_list' above
            temp_view_name = schema+"."+table+"_checkout_temp_view"
            temp_view_names.append(temp_view_name)
            pcur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = \'"+schema+"\' AND table_name   = \'"+table+"\'")
            column_list = pcur.fetchall()
            new_columns_str = preserve_fid( pkey, column_list)
            view_str = "CREATE OR REPLACE VIEW "+temp_view_name+" AS SELECT "+new_columns_str+" FROM " +schema+"."+table
            if feature_list:
                view_str = "CREATE OR REPLACE VIEW "+temp_view_name+" AS SELECT "+new_columns_str+" FROM " +schema+"."+table+" WHERE "+pkey+' in ('+",".join([str(feature_list[i]) for i in range(0, len(feature_list))])+')'
            pcur.execute(view_str)
            pcur.commit()
            cmd[7] = temp_view_name

            os.system(' '.join(cmd))

            # save target revision in a table if not in there
            scur = Db(dbapi2.connect(sqlite_filename))
            scur.execute("INSERT INTO initial_revision"
                    "(rev, branch, table_schema, table_name, max_pk) "
                    "VALUES ("+str(current_rev)+", '"+branch+"', '"+
                    schema+"', '"+table+"', "+str(max_pg_pk)+")" )
            scur.commit()
            scur.close()

        scur = Db(dbapi2.connect(sqlite_filename))

        # create views and triggers in spatilite db
        scur.execute("PRAGMA table_info("+table+")")
        cols = ""
        newcols = ""
        hcols = ['OGC_FID'] + sum([[brch+'_rev_begin', brch+'_rev_end',
                brch+'_parent', brch+'_child'] for brch in pg_branches( pcur, schema ) ],[])
        for res in scur.fetchall():
            if res[1] not in hcols :
                cols += quote_ident(res[1]) + ", "
                newcols += "new."+quote_ident(res[1])+", "
        cols = cols[:-2]
        newcols = newcols[:-2] # remove last coma

        scur.execute( "CREATE VIEW "+table+"_view "+"AS "
            "SELECT ROWID AS ROWID, OGC_FID, "+cols+" "
            "FROM "+table+" WHERE "+branch+"_rev_end IS NULL "
            "AND "+branch+"_rev_begin IS NOT NULL")

        max_fid_sub = ("( SELECT MAX(max_fid) FROM ( SELECT MAX(OGC_FID) AS "
            "max_fid FROM "+table+" UNION SELECT max_pk AS max_fid "
            "FROM initial_revision WHERE table_name = '"+table+"') )")
        current_rev_sub = ("(SELECT rev FROM initial_revision "
            "WHERE table_name = '"+table+"')")

        scur.execute("DELETE FROM views_geometry_columns "
            "WHERE view_name = '"+table+"_view'")
        if 'GEOMETRY' in cols:
            scur.execute("INSERT INTO views_geometry_columns "
                    "(view_name, view_geometry, view_rowid, "
                        "f_table_name, f_geometry_column, read_only) "
                    "VALUES"+"('"+table+"_view', 'geometry', 'rowid', '"
                    +table+"', 'geometry', 0)")

        # when we edit something old, we insert and update parent
        scur.execute(
        "CREATE TRIGGER update_old_"+table+" "
            "INSTEAD OF UPDATE ON "+table+"_view "
            "WHEN (SELECT COUNT(*) FROM "+table+" "
            "WHERE OGC_FID = new.OGC_FID "
            "AND ("+branch+"_rev_begin <= "+current_rev_sub+" ) ) \n"
            "BEGIN\n"
            "INSERT INTO "+table+" "
            "(OGC_FID, "+cols+", "+branch+"_rev_begin, "
             +branch+"_parent) "
            "VALUES "
            "("+max_fid_sub+"+1, "+newcols+", "+current_rev_sub+"+1, "
              "old.OGC_FID);\n"
            "UPDATE "+table+" SET "+branch+"_rev_end = "+current_rev_sub+", "
            +branch+"_child = "+max_fid_sub+" WHERE OGC_FID = old.OGC_FID;\n"
            "END")
        # when we edit something new, we just update
        scur.execute("CREATE TRIGGER update_new_"+table+" "
        "INSTEAD OF UPDATE ON "+table+"_view "
              "WHEN (SELECT COUNT(*) FROM "+table+" "
              "WHERE OGC_FID = new.OGC_FID AND ("+branch+"_rev_begin > "
              +current_rev_sub+" ) ) \n"
              "BEGIN\n"
                "REPLACE INTO "+table+" "
                "(OGC_FID, "+cols+", "+branch+"_rev_begin, "+branch+"_parent) "
                "VALUES "
                "(new.OGC_FID, "+newcols+", "+current_rev_sub+"+1, (SELECT "
                +branch+"_parent FROM "+table+
                " WHERE OGC_FID = new.OGC_FID));\n"
              "END")

        scur.execute("CREATE TRIGGER insert_"+table+" "
        "INSTEAD OF INSERT ON "+table+"_view\n"
            "BEGIN\n"
                "INSERT INTO "+table+" "+
                "(OGC_FID, "+cols+", "+branch+"_rev_begin) "
                "VALUES "
                "("+max_fid_sub+"+1, "+newcols+", "+current_rev_sub+"+1);\n"
            "END")

        scur.execute("CREATE TRIGGER delete_"+table+" "
        "INSTEAD OF DELETE ON "+table+"_view\n"
            "BEGIN\n"
              # update it if its old
                "UPDATE "+table+" "
                    "SET "+branch+"_rev_end = "+current_rev_sub+" "
                    "WHERE OGC_FID = old.OGC_FID "
                    "AND "+branch+"_rev_begin < "+current_rev_sub+"+1;\n"
              # delete it if its new and remove it from child
                "UPDATE "+table+" "
                    "SET "+branch+"_child = NULL "
                    "WHERE "+branch+"_child = old.OGC_FID "
                    "AND "+branch+"_rev_begin = "+current_rev_sub+"+1;\n"
                "DELETE FROM "+table+" "
                    "WHERE OGC_FID = old.OGC_FID "
                    "AND "+branch+"_rev_begin = "+current_rev_sub+"+1;\n"
            "END")

        scur.commit()
        scur.close()
    # Remove temp views after sqlite file is written
    if feature_list:
        for i in temp_view_names:
            del_view_str = "DROP VIEW IF EXISTS " + i
            pcur.execute(del_view_str)
            pcur.commit()
    pcur.close()

def update(sqlite_filename, pg_conn_info):
    """merge modifications since last update into working copy"""
    print "update"
    if unresolved_conflicts(sqlite_filename):
        raise RuntimeError("There are unresolved conflicts in "
                +sqlite_filename)
    # get the target revision from the spatialite db
    # create the diff in postgres
    # load the diff in spatialite
    # detect conflicts and create conflict layers
    # merge changes and update target_revision
    # delete diff

    scur = Db(dbapi2.connect(sqlite_filename))
    scur.execute("SELECT rev, branch, table_schema, table_name, max_pk "
        "FROM initial_revision")
    versioned_layers = scur.fetchall()

    for [rev, branch, table_schema, table, current_max_pk] in versioned_layers:
        pcur = Db(psycopg2.connect(pg_conn_info))
        pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions "
            "WHERE branch = '"+branch+"'")
        [max_rev] = pcur.fetchone()
        if max_rev == rev:
            print ("Nothing new in branch "+branch+" in "+table_schema+"."
                +table+" since last update")
            pcur.close()
            continue

        # get the max pkey
        pkey = pg_pk( pcur, table_schema, table )
        pgeom = pg_geom( pcur, table_schema, table )
        pcur.execute("SELECT MAX("+pkey+") FROM "+table_schema+"."+table)
        [max_pg_pk] = pcur.fetchone()
        if not max_pg_pk :
            max_pg_pk = 0

        # create the diff
        diff_schema = (table_schema+"_"+branch+"_"+str(rev)+
            "_to_"+str(max_rev)+"_diff")
        pcur.execute("SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = '"+diff_schema+"'")
        if not pcur.fetchone():
            pcur.execute("CREATE SCHEMA "+diff_schema)

        other_branches = pg_branches( pcur, table_schema ).remove(branch)
        other_branches = other_branches if other_branches else []
        other_branches_columns = sum([
            [brch+'_rev_begin', brch+'_rev_end',
            brch+'_parent', brch+'_child']
            for brch in other_branches], [])
        pcur.execute("SELECT column_name "
                "FROM information_schema.columns "
                "WHERE table_schema = '"+table_schema+"' "
                "AND table_name = '"+table+"'")
        cols = ""
        for col in pcur.fetchall():
            if col[0] != pgeom and col[0] not in other_branches_columns:
                cols += quote_ident(col[0])+", "
        cols = cols[:-2] # remove last coma and space

        pcur.execute("SELECT srid, type "
            "FROM geometry_columns "
            "WHERE f_table_schema = '"+table_schema+
            "' AND f_table_name ='"+table+"' AND f_geometry_column = '"+pgeom+"'")
        srid_type = pcur.fetchone()
        [srid, geom_type] = srid_type if srid_type else [None, None]
        pcur.execute( "DROP TABLE IF EXISTS "+diff_schema+"."+table+"_diff")
        geom = (", "+pgeom+"::geometry('"+geom_type+"', "+str(srid)+") "
            "AS "+pgeom) if pgeom else ''
        pcur.execute( "CREATE TABLE "+diff_schema+"."+table+"_diff AS "
                "SELECT "+cols+geom+" "
                "FROM "+table_schema+"."+table+" "
                "WHERE "+branch+"_rev_end >= "+str(rev)+" "
                "OR "+branch+"_rev_begin > "+str(rev))
        pcur.execute( "ALTER TABLE "+diff_schema+"."+table+"_diff "
                "ADD CONSTRAINT "+table+"_"+branch+"_pk_pk "
                "PRIMARY KEY ("+pkey+")")
        pcur.commit()

        scur.execute("DROP TABLE IF EXISTS "+table+"_diff")
        scur.execute("DROP TABLE IF EXISTS idx_"+table+"_diff_GEOMETRY")
        scur.execute("DELETE FROM geometry_columns "
            "WHERE f_table_name = '"+table+"_diff'")
        scur.commit()

        # import the diff to spatialite
        cmd = ['ogr2ogr',
                '-preserve_fid',
                '-f', 'SQLite',
                '-update',
                '"' + sqlite_filename + '"',
                'PG:"'+pg_conn_info+'"',
                diff_schema+'.'+table+"_diff",
                '-nln', table+"_diff"]
        print ' '.join(cmd)
        os.system(' '.join(cmd))

        # cleanup in postgis
        pcur.execute("DROP SCHEMA "+diff_schema+" CASCADE")
        pcur.commit()
        pcur.close()

        scur.execute("PRAGMA table_info("+table+")")
        cols = ""
        for col in scur.fetchall():
            cols += quote_ident(col[1])+", "
        cols = cols[:-2] # remove last coma and space

        # update the initial revision
        scur.execute("UPDATE initial_revision "
            "SET rev = "+str(max_rev)+", max_pk = "+str(max_pg_pk)+" "
            "WHERE table_name = '"+table+"'")

        scur.execute("UPDATE "+table+" "
                "SET "+branch+"_rev_end = "+str(max_rev)+" "
                "WHERE "+branch+"_rev_end = "+str(rev))
        scur.execute("UPDATE "+table+" "
                "SET "+branch+"_rev_begin = "+str(max_rev+1)+" "
                "WHERE "+branch+"_rev_begin = "+str(rev+1))

        # we cannot add constrain to the spatialite db in order to have
        # spatialite update parent and child when we bump inserted pkey
        # above the max pkey in the diff we must do this manually
        bump = max_pg_pk - current_max_pk
        assert( bump >= 0)
        # now bump the pks of inserted rows in working copy
        # note that to do that, we need to set a negative value because
        # the UPDATE is not implemented correctly according to:
        # http://stackoverflow.com/questions/19381350/simulate-order-by-in-sqlite-update-to-handle-uniqueness-constraint
        scur.execute("UPDATE "+table+" "
                "SET OGC_FID = -OGC_FID  "
                "WHERE "+branch+"_rev_begin = "+str(max_rev+1))
        scur.execute("UPDATE "+table+" "
            "SET OGC_FID = "+str(bump)+"-OGC_FID WHERE OGC_FID < 0")
        # and bump the pkey in the child field
        # not that we don't care for nulls since adding something
        # to null is null
        scur.execute("UPDATE "+table+" "
                "SET "+branch+"_child = "+branch+"_child  + "+str(bump)+" "
                "WHERE "+branch+"_rev_end = "+str(max_rev))

        # detect conflicts: conflict occur if two lines with the same pkey have
        # been modified (i.e. have a non null child) or one has been removed
        # and the other modified
        scur.execute("DROP VIEW  IF EXISTS "+table+"_conflicts_ogc_fid")
        scur.execute("CREATE VIEW "+table+"_conflicts_ogc_fid AS "
            "SELECT DISTINCT sl.OGC_FID as conflict_deleted_fid "
            "FROM "+table+" AS sl, "+table+"_diff AS pg "
            "WHERE sl.OGC_FID = pg.OGC_FID "
                "AND sl."+branch+"_child != pg."+branch+"_child")
        scur.execute("SELECT conflict_deleted_fid "
            "FROM  "+table+"_conflicts_ogc_fid" )
        if scur.fetchone():
            print "there are conflicts"
            # add layer for conflicts
            scur.execute("DROP TABLE IF EXISTS "+table+"_conflicts ")
            scur.execute("CREATE TABLE "+table+"_conflicts AS "
                # insert new features from mine
                "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, "
                "'modified' AS action, "+cols+" "
                "FROM "+table+", "+table+"_conflicts_ogc_fid AS cflt "
                "WHERE OGC_FID = (SELECT "+branch+"_child FROM "+table+" "
                                     "WHERE OGC_FID = conflict_deleted_fid) "
                "UNION ALL "
                # insert new features from theirs
                "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, "
                "'modified' AS action, "+cols+" "
                "FROM "+table+"_diff "+", "+table+"_conflicts_ogc_fid AS cflt "
                "WHERE OGC_FID = (SELECT "+branch+"_child FROM "+table+"_diff "
                                     "WHERE OGC_FID = conflict_deleted_fid) "
                 # insert deleted features from mine
                "UNION ALL "
                "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, "
                "'deleted' AS action, "+cols+" "
                "FROM "+table+", "+table+"_conflicts_ogc_fid AS cflt "
                "WHERE OGC_FID = conflict_deleted_fid "
                "AND "+branch+"_child IS NULL "
                 # insert deleted features from theirs
                "UNION ALL "
                "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, "
                "'deleted' AS action, "+cols+" "
                "FROM "+table+"_diff, "+table+"_conflicts_ogc_fid AS cflt "
                "WHERE OGC_FID = conflict_deleted_fid "
                "AND "+branch+"_child IS NULL" )

            # identify conflicts for deleted
            scur.execute("UPDATE "+table+"_conflicts "
                "SET conflict_id = OGC_FID "+ "WHERE action = 'deleted'")

            # now follow child if any for 'theirs' 'modified' since several
            # edition could be made we want the very last child
            while True:
                scur.execute("SELECT conflict_id, OGC_FID, "+branch+"_child "
                    "FROM "+table+"_conflicts WHERE origin='theirs' "
                    "AND action='modified' AND "+branch+"_child IS NOT NULL")
                res = scur.fetchall()
                if not res :
                    break
                # replaces each entries by it's child
                for [cflt_id, fid, child] in res:
                    scur.execute("DELETE FROM "+table+"_conflicts "
                        "WHERE OGC_FID = "+str(fid))
                    scur.execute("INSERT INTO "+table+"_conflicts "
                        "SELECT "+str(cflt_id)+" AS conflict_id, "
                        "'theirs' AS origin, 'modified' AS action, "+cols+" "
                        "FROM "+table+"_diff "
                        "WHERE OGC_FID = "+str(child)+" "
                        "AND "+branch+"_rev_end IS NULL" )
                    scur.execute("INSERT INTO "+table+"_conflicts "
                        "SELECT "+str(cflt_id)+" AS conflict_id, "
                        "'theirs' AS origin, 'deleted' AS action, "+cols+" "
                        "FROM "+table+"_diff "
                        "WHERE OGC_FID = "+str(child)+" "
                        "AND "+branch+"_rev_end IS NOT NULL" )

            scur.execute("DELETE FROM geometry_columns "
                "WHERE f_table_name = '"+table+"_conflicts'")
            if geom:
                scur.execute("SELECT RecoverGeometryColumn("
                "'"+table+"_conflicts', 'GEOMETRY', "
                "(SELECT srid FROM geometry_columns "
                "WHERE f_table_name='"+table+"'), "
                "(SELECT GeometryType(geometry) FROM "+table+" LIMIT 1), "
                "'XY')")

            scur.execute("CREATE UNIQUE INDEX IF NOT EXISTS "
                +table+"_conflicts_idx ON "+table+"_conflicts(OGC_FID)")

            # create trigers such that on delete the conflict is resolved
            # if we delete 'theirs', we set their child to our fid and
            # their rev_end if we delete 'mine'... well, we delete 'mine'

            scur.execute("DROP TRIGGER IF EXISTS delete_"+table+"_conflicts")
            scur.execute("CREATE TRIGGER delete_"+table+"_conflicts "
            "AFTER DELETE ON "+table+"_conflicts\n"
                "BEGIN\n"
                    "DELETE FROM "+table+" "
                    "WHERE OGC_FID = old.OGC_FID AND old.origin = 'mine';\n"

                    "UPDATE "+table+" "
                    "SET "+branch+"_child = (SELECT OGC_FID "
                    "FROM "+table+"_conflicts "
                    "WHERE origin = 'mine' "
                    "AND conflict_id = old.conflict_id), "
                    +branch+"_rev_end = "+str(max_rev)+" "
                    "WHERE OGC_FID = old.OGC_FID AND old.origin = 'theirs';\n"

                    "UPDATE "+table+" "
                    "SET "+branch+"_parent = old.OGC_FID "
                    "WHERE OGC_FID = (SELECT OGC_FID "
                    "FROM "+table+"_conflicts WHERE origin = 'mine' "
                    "AND conflict_id = old.conflict_id) "
                    "AND old.origin = 'theirs';\n"

                    "DELETE FROM "+table+"_conflicts "
                    "WHERE conflict_id = old.conflict_id;\n"
                "END")

            scur.commit()

        scur.execute("CREATE UNIQUE INDEX IF NOT EXISTS "
            +table+"_diff_idx ON "+table+"_diff(OGC_FID)")
        # insert and replace all in diff
        scur.execute("INSERT OR REPLACE INTO "+table+" ("+cols+") "
            "SELECT "+cols+" FROM "+table+"_diff")

    scur.commit()
    scur.close()

def late(sqlite_filename, pg_conn_info):
    """Return 0 if up to date, the number of commits in between otherwise"""
    scur = Db(dbapi2.connect(sqlite_filename))
    scur.execute("SELECT rev, branch, table_schema "
        "FROM initial_revision")
    versioned_layers = scur.fetchall()
    if not versioned_layers:
        raise RuntimeError("Cannot find versioned layer in "+sqlite_filename)

    late_by = 0

    for [rev, branch, table_schema] in versioned_layers:
        pcur = Db(psycopg2.connect(pg_conn_info))
        pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions "
            "WHERE branch = '"+branch+"'")
        [max_rev] = pcur.fetchone()
        late_by = max(max_rev - rev, late_by)

    return late_by

def revision( sqlite_filename ):
    """returns the revision the working copy was created from plus one"""
    scur = Db(dbapi2.connect(sqlite_filename))
    scur.execute("SELECT rev "+ "FROM initial_revision")
    rev = 0
    for [res] in scur.fetchall():
        if rev:
            assert( res == rev)
        else :
            rev = res
    scur.close()
    return rev+ 1

def commit(sqlite_filename, commit_msg, pg_conn_info,commit_pg_user = ''):
    """merge modifications into database
    returns the number of updated layers"""
    # get the target revision from the spatialite db
    # create the diff in postgres
    # load the diff in spatialite
    # detect conflicts
    # merge changes and update target_revision
    # delete diff

    unresolved = unresolved_conflicts(sqlite_filename)
    if unresolved:
        raise RuntimeError("There are unresolved conflicts in "
            +sqlite_filename+" for table(s) "+', '.join(unresolved) )

    late_by = late(sqlite_filename, pg_conn_info)
    if late_by:
        raise RuntimeError("Working copy "+sqlite_filename+
                " is not up to date. "
                "It's late by "+str(late_by)+" commit(s).\n\n"
                "Please update before commiting your modifications")

    scur = Db(dbapi2.connect(sqlite_filename))
    scur.execute("SELECT rev, branch, table_schema, table_name "
        "FROM initial_revision")
    versioned_layers = scur.fetchall()

    if not versioned_layers:
        raise RuntimeError("Cannot find a versioned layer in "+sqlite_filename)

    schema_list = {} # for final cleanup
    nb_of_updated_layer = 0
    next_rev = 0
    for [rev, branch, table_schema, table] in versioned_layers:
        diff_schema = (table_schema+"_"+branch+"_"+str(rev)+
                "_to_"+str(rev+1)+"_diff")

        if next_rev:
            assert( next_rev == rev + 1 )
        else:
            next_rev = rev + 1

        scur.execute( "DROP TABLE IF EXISTS "+table+"_diff")

        # note, creating the diff table dirrectly with
        # CREATE TABLE... AS SELECT won't work
        # types get fubared in the process
        # therefore we copy the creation statement from
        # spatialite master and change the
        # table name ta obtain a similar table, we add the geometry column
        # to geometry_columns manually and we insert the diffs
        scur.execute("SELECT sql FROM sqlite_master "
            "WHERE tbl_name = '"+table+"' AND type = 'table'")
        [sql] = scur.fetchone()
        sql = unicode.replace(sql, table, table+"_diff", 1)
        scur.execute(sql)
        geom = (sql.find('GEOMETRY') != -1)
        scur.execute("DELETE FROM geometry_columns "
            "WHERE f_table_name = '"+table+"_diff'")
        if geom:
            scur.execute("INSERT INTO geometry_columns "
                "SELECT '"+table+"_diff', 'geometry', geometry_type, "
                "coord_dimension, srid, spatial_index_enabled "
                "FROM geometry_columns WHERE f_table_name = '"+table+"'")
        scur.execute( "INSERT INTO "+table+"_diff "
                "SELECT * "
                "FROM "+table+" "
                "WHERE "+branch+"_rev_end = "+str(rev)+" "
                "OR "+branch+"_rev_begin > "+str(rev))
        scur.execute( "SELECT OGC_FID FROM "+table+"_diff")
        there_is_something_to_commit = scur.fetchone()
        print "there_is_something_to_commit ", there_is_something_to_commit
        scur.commit()

        # Better if we could have a QgsDataSourceURI.username()
        try:
            pg_username = pg_conn_info.split(' ')[3].replace("'","").split('=')[1]
        except (IndexError):
            pg_username = ''

        pcur = Db(psycopg2.connect(pg_conn_info))
        pg_users_list = get_pg_users_list(pg_conn_info)
        pkey = pg_pk( pcur, table_schema, table )
        pgeom = pg_geom( pcur, table_schema, table )

        # import layers in postgis schema
        pcur.execute("SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = '"+diff_schema+"'")
        if not pcur.fetchone():
            schema_list[diff_schema] = pg_conn_info
            pcur.execute("CREATE SCHEMA "+diff_schema)
        pcur.execute( "DROP TABLE IF EXISTS "+diff_schema+"."+table+"_diff")
        pcur.commit()
        cmd = ['ogr2ogr',
                '-preserve_fid',
                '-f',
                'PostgreSQL',
                'PG:"'+pg_conn_info+'"',
                '-lco',
                'FID='+pkey,
                '"' + sqlite_filename + '"',
                table+"_diff",
                '-nln', diff_schema+'.'+table+"_diff"]
        if pgeom:
            cmd.insert(5, '-lco')
            cmd.insert(6, 'GEOMETRY_NAME='+pgeom)

        print ' '.join(cmd)
        os.system(' '.join(cmd))

        # remove dif table and geometry column
        scur.execute("DELETE FROM geometry_columns "
            "WHERE f_table_name = '"+table+"_diff'")
        scur.execute("DROP TABLE "+table+"_diff")


        if not there_is_something_to_commit:
            print "nothing to commit for ", table
            pcur.close()
            continue

        nb_of_updated_layer += 1

        pcur.execute("SELECT rev FROM "+table_schema+".revisions "
            "WHERE rev = "+str(rev+1))
        if not pcur.fetchone():
            print "inserting rev ", str(rev+1)
            pcur.execute("INSERT INTO "+table_schema+".revisions "
                "(rev, commit_msg, branch, author) "
                "VALUES ("+str(rev+1)+", '"+escape_quote(commit_msg)+"', '"+branch+"',"
                "'"+os_info()+":"+get_username()+"."+pg_username+"."+commit_pg_user+"')")

        # TODO remove when ogr2ogr will be able to convert multiple geom column
        # from postgis to spatialite
        geoms = pg_geoms( pcur, table_schema, table )
        if len(geoms) > 1:
            dest_geom = ''
            src_geom = ''
            for geo in geoms:
                if geo != pgeom:
                    dest_geom += geo+', '
                    src_geom += 'src.'+geo+', '
            dest_geom = dest_geom[:-2]
            src_geom = src_geom[:-2]
            pgeom = pg_geom( pcur, table_schema, table )
            pcur.execute("SELECT AddGeometryColumn('"+diff_schema+"', '"+table+"_diff', "
                "'"+geo+"', srid, type, coord_dimension) FROM geometry_columns "
                "WHERE f_table_name = '"+table+"' "
                "AND f_table_schema = '"+table_schema+"' "
                "AND f_geometry_column != '"+pgeom+"'")
            pcur.execute("UPDATE "+diff_schema+"."+table+"_diff AS dest "
                "SET ("+dest_geom+") =  ("+src_geom+") "
                "FROM "+table_schema+"."+table+" AS src "
                "WHERE dest."+branch+"_rev_begin = "+str(rev+1)+" "
                "AND src."+pkey+" = dest."+branch+"_parent")

        other_branches = pg_branches( pcur, table_schema ).remove(branch)
        other_branches = other_branches if other_branches else []
        other_branches_columns = sum([
            [brch+'_rev_begin', brch+'_rev_end',
            brch+'_parent', brch+'_child']
            for brch in other_branches], [])
        pcur.execute("SELECT column_name, data_type "
                "FROM information_schema.columns "
                "WHERE table_schema = '"+table_schema+"' "
                "AND table_name = '"+table+"'")
        cols = ""
        cols_cast = ""
        for col in pcur.fetchall():
            if col[0] not in other_branches_columns:
                cols += quote_ident(col[0])+", "
                if col[1] != 'ARRAY':
                    cast = "::"+col[1] if col[1] != 'USER-DEFINED' else ""
                    cols_cast += quote_ident(col[0])+cast+", "
                else :
                    cols_cast += ("regexp_replace(regexp_replace("
                            +col[0]+",'^\(.*:','{'),'\)$','}')::"
                            +pg_array_elem_type(pcur,
                                table_schema, table, col[0])+"[], ")
        cols = cols[:-2] # remove last coma and space
        cols_cast = cols_cast[:-2] # remove last coma and space
        # insert inserted and modified
        pcur.execute("INSERT INTO "+table_schema+"."+table+" ("+cols+") "
            "SELECT "+cols_cast+" FROM "+diff_schema+"."+table+"_diff "
            "WHERE "+branch+"_rev_begin = "+str(rev+1))

        # update deleted and modified
        pcur.execute("UPDATE "+table_schema+"."+table+" AS dest "
                "SET ("+branch+"_rev_end, "+branch+"_child)"
                "=(src."+branch+"_rev_end, src."+branch+"_child) "
                "FROM "+diff_schema+"."+table+"_diff AS src "
                "WHERE dest."+pkey+" = src."+pkey+" "
                "AND src."+branch+"_rev_end = "+str(rev))

        pcur.commit()
        pcur.close()

        scur.commit()

    if nb_of_updated_layer:
        for [rev, branch, table_schema, table] in versioned_layers:
            pcur = Db(psycopg2.connect(pg_conn_info))
            pkey = pg_pk( pcur, table_schema, table )
            pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions")
            [rev] = pcur.fetchone()
            pcur.execute("SELECT MAX("+pkey+") FROM "+table_schema+"."+table)
            [max_pk] = pcur.fetchone()
            if not max_pk :
                max_pk = 0
            pcur.close()
            scur.execute("UPDATE initial_revision "
                "SET rev = "+str(rev)+", max_pk = "+str(max_pk)+" "
                "WHERE table_schema = '"+table_schema+"' "
                "AND table_name = '"+table+"' "
                "AND branch = '"+branch+"'")

    scur.commit()
    scur.close()

    # cleanup diffs in postgis
    for schema, conn_info in schema_list.iteritems():
        pcur = Db(psycopg2.connect(conn_info))
        pcur.execute("DROP SCHEMA "+schema+" CASCADE")
        pcur.commit()
        pcur.close()

    return nb_of_updated_layer

def historize( pg_conn_info, schema ):
    """Create historisation for the given schema"""
    if not schema:
        raise RuntimeError("no schema specified")
    pcur = Db(psycopg2.connect(pg_conn_info))

    pcur.execute("CREATE TABLE "+schema+".revisions ("
        "rev serial PRIMARY KEY, "
        "commit_msg varchar, "
        "branch varchar DEFAULT 'trunk', "
        "date timestamp DEFAULT current_timestamp, "
        "author varchar)")
    pcur.commit()
    pcur.close()
    add_branch( pg_conn_info, schema, 'trunk', 'initial commit' )

def add_branch( pg_conn_info, schema, branch, commit_msg,
        base_branch='trunk', base_rev='head' ):
    """Create a new branch (add 4 columns to tables)"""
    pcur = Db(psycopg2.connect(pg_conn_info))

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
    print 'max rev = ', max_rev

    pcur.execute("INSERT INTO "+schema+".revisions(rev, branch, commit_msg ) "
        "VALUES ("+str(max_rev+1)+", '"+branch+"', '"+escape_quote(commit_msg)+"')")
    pcur.execute("CREATE SCHEMA "+schema+"_"+branch+"_rev_head")

    history_columns = sum([
        [brch+'_rev_end', brch+'_rev_begin',
        brch+'_child', brch+'_parent' ] for brch in pg_branches( pcur, schema )],[])

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
            pkey = pg_pk( pcur, schema, table )
        except:
            if 'VERSIONING_NO_PK' in os.environ and os.environ['VERSIONING_NO_PK'] == 'skip':
                print schema+'.'+table+' has no primary key, skipping'
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
                cols = quote_ident(col)+", "+cols
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

    pcur = Db(psycopg2.connect(pg_conn_info))

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
    pcur = Db(psycopg2.connect(pg_conn_info))

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
    pcur = Db(psycopg2.connect(pg_conn_info))

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
        brch+'_child', brch+'_parent' ] for brch in pg_branches( pcur, schema )],[])

    rev_schema = schema+"_"+branch+"_rev_"+str(rev)

    pcur.execute("SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name = '"+rev_schema+"'")
    if pcur.fetchone():
        print rev_schema, ' already exists'
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
                cols = quote_ident(col)+", "+cols
        cols = cols[:-2] # remove last coma and space
        pcur.execute("CREATE VIEW "+rev_schema+"."+table+" "+security+" AS "
           "SELECT "+cols+" FROM "+schema+"."+table+" "
           "WHERE ("+branch+"_rev_end IS NULL "
                   "OR "+branch+"_rev_end >= "+str(rev)+") "
           "AND "+branch+"_rev_begin <= "+str(rev))

    pcur.commit()
    pcur.close()

def pg_branches(pcur, schema):
    """returns a list of branches for this schema"""
    pcur.execute("SELECT DISTINCT branch FROM "+schema+".revisions")
    return [ res for [res] in pcur.fetchall() ]

def revisions(pg_conn_info, schema):
    """returns a list of revisions for this schema"""
    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT rev FROM "+schema+".revisions")
    revs = []
    for [res] in pcur.fetchall():
        revs.append(res)
    pcur.close()
    return revs

# functions checkout, update and commit for a posgres working copy
# we don't want to duplicate data
# we need the initial_revision table all the same
# for each table we need a diff and a view and triggers

def pg_checkout(pg_conn_info, pg_table_names, working_copy_schema):
    """create postgres working copy from versioned database tables
    pg_table_names must be complete schema.table names
    the schema name must end with _branch_rev_head
    the working_copy_schema must not exist
    the views and triggers for local edition will be created
    along with the tables and triggers for conflict resolution"""
    pcur = Db(psycopg2.connect(pg_conn_info))
    wcs = working_copy_schema
    pcur.execute("SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name = '"+wcs+"'")
    if pcur.fetchone():
        raise RuntimeError("Schema "+wcs+" already exists")

    for pg_table_name in pg_table_names:
        [schema, table] = pg_table_name.split('.')
        if not ( schema and table and schema[-9:] == "_rev_head"):
            raise RuntimeError("Schema names must end with suffix "
                "_branch_rev_head")


    pcur.execute("CREATE SCHEMA "+wcs)

    first_table = True
    for pg_table_name in pg_table_names:
        [schema, table] = pg_table_name.split('.')
        [schema, sep, branch] = schema[:-9].rpartition('_')
        del sep

        pkey = pg_pk( pcur, schema, table )
        history_columns = [pkey] + sum([
            [brch+'_rev_end', brch+'_rev_begin',
            brch+'_child', brch+'_parent' ] for brch in pg_branches( pcur, schema )],[])

        # fetch the current rev
        pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
        current_rev = int(pcur.fetchone()[0])

        # max pkey for this table
        pcur.execute("SELECT MAX("+pkey+") FROM "+schema+"."+table)
        [max_pg_pk] = pcur.fetchone()
        if not max_pg_pk :
            max_pg_pk = 0
        if first_table:
            first_table = False
            pcur.execute("CREATE TABLE "+wcs+".initial_revision AS SELECT "
                    +str(current_rev)+" AS rev, '"
                    +branch+"'::varchar AS branch, '"
                    +schema+"'::varchar AS table_schema, '"
                    +table+"'::varchar AS table_name, "
                    +str(max_pg_pk)+" AS max_pk")
        else:
            pcur.execute("INSERT INTO "+wcs+".initial_revision"
            "(rev, branch, table_schema, table_name, max_pk) "
            "VALUES ("+str(current_rev)+", '"+branch+"', '"+schema+"', "
                "'"+table+"', "+str(max_pg_pk)+")" )

        # create diff, views and triggers
        pcur.execute("SELECT column_name "
                "FROM information_schema.columns "
                "WHERE table_schema = '"+schema+"' "
                "AND table_name = '"+table+"'")
        cols = ""
        newcols = ""
        for [col] in pcur.fetchall():
            if col not in history_columns:
                cols = quote_ident(col)+", "+cols
                newcols = "new."+quote_ident(col)+", "+newcols
        cols = cols[:-2]
        newcols = newcols[:-2] # remove last coma and space
        hcols = (pkey+", "+branch+"_rev_begin, "+branch+"_rev_end, "
                +branch+"_parent, "+branch+"_child")

        pcur.execute("CREATE TABLE "+wcs+"."+table+"_diff "
                "AS SELECT "+cols+" FROM "+schema+"."+table+" WHERE False")

        pcur.execute("ALTER TABLE "+wcs+"."+table+"_diff "
            "ADD COLUMN "+pkey+" integer PRIMARY KEY, "
            "ADD COLUMN "+branch+"_rev_begin integer, "
            "ADD COLUMN "+branch+"_rev_end   integer, "
            "ADD COLUMN "+branch+"_parent    integer,"
            "ADD COLUMN "+branch+"_child     integer "
            "REFERENCES "+wcs+"."+table+"_diff("+pkey+") "
            "ON UPDATE CASCADE ON DELETE CASCADE")


        current_rev_sub = "(SELECT MAX(rev) FROM "+wcs+".initial_revision)"
        pcur.execute("CREATE VIEW "+wcs+"."+table+"_view AS "
                "SELECT "+pkey+", "+cols+" "
                "FROM (SELECT "+cols+", "+hcols+" FROM "+wcs+"."+table+"_diff "
                        "WHERE ("+branch+"_rev_end IS NULL "
                        "OR "+branch+"_rev_end >= "+current_rev_sub+"+1 ) "
                        "AND "+branch+"_rev_begin IS NOT NULL "
                        "UNION "
                        "(SELECT DISTINCT ON ("+pkey+") "+cols+", t."+hcols+" "
                        "FROM "+schema+"."+table+" AS t "
                        "LEFT JOIN (SELECT "+pkey+" FROM "+wcs+"."+table+"_diff) "
                        "AS d "
                        "ON t."+pkey+" = d."+pkey+" "
                        "WHERE d."+pkey+" IS NULL "
                        "AND t."+branch+"_rev_begin <= "+current_rev_sub+" "
                        "AND ((t."+branch+"_rev_end IS NULL "
                            "OR t."+branch+"_rev_end >= "+current_rev_sub+") "
                            "AND t."+branch+"_rev_begin IS NOT NULL ))"
                        ") AS src ")

        max_fid_sub = ("( SELECT MAX(max_fid) FROM ( SELECT MAX("+pkey+") "
            "AS max_fid FROM "+wcs+"."+table+"_diff "
            "UNION SELECT max_pk AS max_fid "
            "FROM "+wcs+".initial_revision "
            "WHERE table_name = '"+table+"') AS src )")

        pcur.execute("CREATE OR REPLACE FUNCTION myprt(error_message text) "
        "RETURNS void as $$\n"
            "begin\n"
                "raise notice '%', error_message;\n"
            "end;\n"
            "$$ language plpgsql;")

        pcur.execute("CREATE FUNCTION "+wcs+".update_"+table+"() "
        "RETURNS trigger AS $$\n"
            "BEGIN\n"
                # when we edit something we already added , we just update
                "UPDATE "+wcs+"."+table+"_diff "
                "SET ("+cols+") = ("+newcols+") "
                "WHERE "+pkey+" = OLD."+pkey+" "
                "AND "+branch+"_rev_begin = "+current_rev_sub+"+1 "
                "AND "
                "(SELECT COUNT(*) FROM "+schema+"."+table+" "
                    "WHERE "+pkey+" = OLD."+pkey+" "
                    "AND "+branch+"_rev_begin <= "+current_rev_sub+" "
                    "AND ("+branch+"_rev_end IS NULL "
                        "OR "+branch+"_rev_end >= "+current_rev_sub+" ) "
                ") = 0 ;"

                # insert the parent in diff if not already there
                "INSERT INTO "+wcs+"."+table+"_diff "
                "("+cols+", "+pkey+", "+branch+"_rev_begin, "
                    +branch+"_rev_end, "+branch+"_parent ) "
                "SELECT "+cols+", "+pkey+", "+branch+"_rev_begin, "
                    +branch+"_rev_end, "+branch+"_parent "
                "FROM "+schema+"."+table+" "
                "WHERE "+pkey+" = OLD."+pkey+" "
                "AND "+branch+"_rev_begin <= "+current_rev_sub+" "
                "AND (SELECT COUNT(*) FROM "+wcs+"."+table+"_diff "
                    "WHERE "+pkey+" = OLD."+pkey+" "
                    "AND "+branch+"_rev_end = "+current_rev_sub+" ) = 0 ;"

                # when we edit something old, we insert new
                "INSERT INTO "+wcs+"."+table+"_diff "
                "("+pkey+", "+cols+", "+branch+"_rev_begin, "+branch+"_parent) "
                "SELECT "+max_fid_sub+"+1, "+newcols+", "
                    +current_rev_sub+"+1, OLD."+pkey+" "
                "WHERE (SELECT COUNT(*) FROM "+schema+"."+table+" "
                    "WHERE "+pkey+" = OLD."+pkey+" "
                    "AND "+branch+"_rev_begin <= "+current_rev_sub+" "
                    "AND ("+branch+"_rev_end IS NULL "
                        "OR "+branch+"_rev_end >= "+current_rev_sub+" ) "
                    ") = 1; "

                # update the parent in diff if it comes from the table
                # (i.e not the diff)
                "UPDATE "+wcs+"."+table+"_diff "
                    "SET ("+branch+"_rev_end, "+branch+"_child) "
                    "= ("+current_rev_sub+", "+max_fid_sub+") "
                    "WHERE "+pkey+" = OLD."+pkey+" "
                    "AND (SELECT COUNT(*) FROM "+schema+"."+table+" "
                    "WHERE "+pkey+" = OLD."+pkey+" "
                    "AND "+branch+"_rev_begin <= "+current_rev_sub+" "
                    "AND ("+branch+"_rev_end IS NULL "
                        "OR "+branch+"_rev_end >= "+current_rev_sub+")) = 1;\n"
                "RETURN NULL;\n"
            "END;\n"
        "$$ LANGUAGE plpgsql;")

        pcur.execute("CREATE TRIGGER update_"+table+" "
            "INSTEAD OF UPDATE ON "+wcs+"."+table+"_view "
            "FOR EACH ROW EXECUTE PROCEDURE "+wcs+".update_"+table+"();")

        pcur.execute("CREATE FUNCTION "+wcs+".insert_"+table+"() "
        "RETURNS trigger AS $$\n"
            "BEGIN\n"
                "INSERT INTO "+wcs+"."+table+"_diff "+
                    "("+pkey+", "+cols+", "+branch+"_rev_begin) "
                    "VALUES "
                    "("+max_fid_sub+"+1, "+newcols+", "+current_rev_sub+"+1);\n"
                "RETURN NULL;\n"
            "END;\n"
        "$$ LANGUAGE plpgsql;")

        pcur.execute("CREATE TRIGGER insert_"+table+" "
            "INSTEAD OF INSERT ON "+wcs+"."+table+"_view "
            "FOR EACH ROW EXECUTE PROCEDURE "+wcs+".insert_"+table+"();")

        pcur.execute("CREATE FUNCTION "+wcs+".delete_"+table+"() "
        "RETURNS trigger AS $$\n"
            "BEGIN\n"
                # insert if not already in diff
                "INSERT INTO "+wcs+"."+table+"_diff "
                    "SELECT "+cols+", "+hcols+" FROM "+schema+"."+table+" "
                    "WHERE "+pkey+" = OLD."+pkey+" "
                    "AND (SELECT COUNT(*) FROM "+wcs+"."+table+"_diff "
                    "WHERE "+pkey+" = OLD."+pkey+") = 0;\n"
                # update if it comes from table (not diff)
                "UPDATE "+wcs+"."+table+"_diff "
                    "SET "+branch+"_rev_end = "+current_rev_sub+" "
                    "WHERE "+pkey+" = OLD."+pkey+" "
                    "AND (SELECT COUNT(*) FROM  "+schema+"."+table+" "
                    "WHERE "+pkey+" = OLD."+pkey+") = 1; "

                # if its just in diff, remove it from child
                "UPDATE "+wcs+"."+table+"_diff "
                    "SET "+branch+"_child = NULL "
                    "WHERE "+branch+"_child = OLD."+pkey+" "
                    "AND (SELECT COUNT(*) FROM  "+schema+"."+table+" "
                    "WHERE "+pkey+" = OLD."+pkey+") = 0;\n"

                # if it's just in diff, delete it
                "DELETE FROM  "+wcs+"."+table+"_diff "
                    "WHERE "+pkey+" = OLD."+pkey+" "
                    "AND (SELECT COUNT(*) FROM  "+schema+"."+table+" "
                    "WHERE "+pkey+" = OLD."+pkey+") = 0;\n"
                "RETURN NULL;\n"
            "END;\n"
        "$$ LANGUAGE plpgsql;")

        pcur.execute("CREATE TRIGGER delete_"+table+" "
            "INSTEAD OF DELETE ON "+wcs+"."+table+"_view "
            "FOR EACH ROW EXECUTE PROCEDURE "+wcs+".delete_"+table+"();")

    pcur.commit()
    pcur.close()

def pg_update(pg_conn_info, working_copy_schema):
    """merge modifications since last update into working copy"""
    print "update"
    wcs = working_copy_schema
    if pg_unresolved_conflicts(pg_conn_info, wcs):
        raise RuntimeError("There are unresolved conflicts in "+wcs)

    # create the diff from previous
    # detect conflicts and create conflict layers
    # merge changes and update target_revision


    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT rev, branch, table_schema, table_name, max_pk "
        "FROM "+wcs+".initial_revision")
    versioned_layers = pcur.fetchall()

    for [rev, branch, table_schema, table, current_max_pk] in versioned_layers:

        pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions "
            "WHERE branch = '"+branch+"'")
        [max_rev] = pcur.fetchone()
        if max_rev == rev:
            print ("Nothing new in branch "+branch+" "
                "in "+table_schema+"."+table+" since last update")
            continue

        # get the max pkey
        pkey = pg_pk( pcur, table_schema, table )
        pgeom = pg_geom( pcur, table_schema, table )
        pcur.execute("SELECT MAX("+pkey+") FROM "+table_schema+"."+table)
        [max_pg_pk] = pcur.fetchone()
        if not max_pg_pk :
            max_pg_pk = 0

        # create the diff
        pcur.execute("SELECT column_name "
                "FROM information_schema.columns "
                "WHERE table_schema = '"+table_schema+"' "
                "AND table_name = '"+table+"'")
        cols = ""
        for col in pcur.fetchall():
            if col[0] != pgeom:
                cols += quote_ident(col[0])+", "
        cols = cols[:-2] # remove last coma and space

        pcur.execute("SELECT srid, type "
            "FROM geometry_columns "
            "WHERE f_table_schema = '"+table_schema+"' "
            "AND f_table_name ='"+table+"' AND f_geometry_column = '"+pgeom+"'")
        srid_type = pcur.fetchone()
        [srid, geom_type] = srid_type if srid_type else [None, None]
        pcur.execute( "DROP TABLE IF EXISTS "+wcs+"."+table+"_update_diff "
            "CASCADE")
        geom = (", "+pgeom+"::geometry('"+geom_type+"', "+str(srid)+") "
            "AS "+pgeom) if pgeom else ''
        pcur.execute( "CREATE TABLE "+wcs+"."+table+"_update_diff AS "
                "SELECT "+cols+geom+" "
                "FROM "+table_schema+"."+table+" "
                "WHERE "+branch+"_rev_end = "+str(rev)+" "
                "OR "+branch+"_rev_begin > "+str(rev))
        pcur.execute( "ALTER TABLE "+wcs+"."+table+"_update_diff "
                "ADD CONSTRAINT "+table+"_"+branch+"_pk_pk "
                "PRIMARY KEY ("+pkey+")")

        # update the initial revision
        pcur.execute("UPDATE "+wcs+".initial_revision "
            "SET rev = "+str(max_rev)+", max_pk = "+str(max_pg_pk)+" "
            "WHERE table_name = '"+table+"'")

        pcur.execute("UPDATE "+wcs+"."+table+"_diff "
                "SET "+branch+"_rev_end = "+str(max_rev)+" "
                "WHERE "+branch+"_rev_end = "+str(rev))
        pcur.execute("UPDATE "+wcs+"."+table+"_diff "
                "SET "+branch+"_rev_begin = "+str(max_rev+1)+" "
                "WHERE "+branch+"_rev_begin = "+str(rev+1))

        bump = max_pg_pk - current_max_pk
        assert( bump >= 0)
        # now bump the pks of inserted rows in working copy
        # parents will be updated thanks to the ON UPDATE CASCADE
        pcur.execute("UPDATE "+wcs+"."+table+"_diff "
                "SET "+pkey+" = -"+pkey+" "
                "WHERE "+branch+"_rev_begin = "+str(max_rev+1))
        pcur.execute("UPDATE "+wcs+"."+table+"_diff "
                "SET "+pkey+" = -"+pkey+" + "+str(bump)+" "
                "WHERE "+branch+"_rev_begin = "+str(max_rev+1))

        # detect conflicts: conflict occur if two lines with the same pkey have
        # been modified (i.e. have a non null child) or one has been removed
        # and the other modified
        pcur.execute("DROP VIEW IF EXISTS "+wcs+"."+table+"_conflicts_pk")
        pcur.execute("CREATE VIEW "+wcs+"."+table+"_conflicts_pk AS "
            "SELECT DISTINCT d."+pkey+" as conflict_deleted_pk "
            "FROM "+wcs+"."+table+"_diff AS d, "
                +wcs+"."+table+"_update_diff AS ud "
            "WHERE d."+pkey+" = ud."+pkey+" "
                "AND (d."+branch+"_child != ud."+branch+"_child "
                "OR (d."+branch+"_child IS NULL "
                    "AND ud."+branch+"_child IS NOT NULL) "
                "OR (d."+branch+"_child IS NOT NULL "
                    "AND ud."+branch+"_child IS NULL)) ")
        pcur.execute("SELECT conflict_deleted_pk "
            "FROM  "+wcs+"."+table+"_conflicts_pk" )
        geom = ', '+pgeom if pgeom else ''
        if pcur.fetchone():
            print "there are conflicts"
            # add layer for conflicts
            pcur.execute("DROP TABLE IF EXISTS "+wcs+"."+table+"_cflt ")
            pcur.execute("CREATE TABLE "+wcs+"."+table+"_cflt AS "
                # insert new features from mine
                "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, "
                    "'modified' AS action, "+cols+geom+" "
                "FROM "+wcs+"."+table+"_diff, "
                    +wcs+"."+table+"_conflicts_pk AS cflt "
                "WHERE "+pkey+" = (SELECT "+branch+"_child "
                                "FROM "+wcs+"."+table+"_diff "
                                "WHERE "+pkey+" = conflict_deleted_pk) "
                "UNION ALL "
                # insert new features from theirs
                "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, "
                    "'modified' AS action, "+cols+geom+" "
                "FROM "+wcs+"."+table+"_update_diff "+", "
                    +wcs+"."+table+"_conflicts_pk AS cflt "
                "WHERE "+pkey+" = (SELECT "+branch+"_child "
                                "FROM "+wcs+"."+table+"_update_diff "
                                "WHERE "+pkey+" = conflict_deleted_pk) "
                 # insert deleted features from mine
                "UNION ALL "
                "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, "
                    "'deleted' AS action, "+cols+geom+" "
                "FROM "+wcs+"."+table+"_diff, "
                    +wcs+"."+table+"_conflicts_pk AS cflt "
                "WHERE "+pkey+" = conflict_deleted_pk "
                "AND "+branch+"_child IS NULL "
                 # insert deleted features from theirs
                "UNION ALL "
                "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, "
                    "'deleted' AS action, "+cols+geom+" "
                "FROM "+wcs+"."+table+"_update_diff, "
                    +wcs+"."+table+"_conflicts_pk AS cflt "
                "WHERE "+pkey+" = conflict_deleted_pk "
                "AND "+branch+"_child IS NULL" )

            # identify conflicts for deleted
            pcur.execute("UPDATE "+wcs+"."+table+"_cflt "
                "SET conflict_id = "+pkey+" "+ "WHERE action = 'deleted'")

            # now follow child if any for 'theirs' 'modified'
            # since several edition could be made
            # we want the very last child
            while True:
                pcur.execute("SELECT conflict_id, "+pkey+", "+branch+"_child "
                    "FROM "+wcs+"."+table+"_cflt "
                    "WHERE origin='theirs' "
                    "AND action='modified' "
                    "AND "+branch+"_child IS NOT NULL")
                res = pcur.fetchall()
                if not res :
                    break
                # replaces each entries by it's child
                for [cflt_id, fid, child] in res:
                    pcur.execute("DELETE FROM "+wcs+"."+table+"_cflt "
                        "WHERE "+pkey+" = "+str(fid))
                    pcur.execute("INSERT INTO "+wcs+"."+table+"_cflt "
                        "SELECT "+str(cflt_id)+" AS conflict_id, "
                            "'theirs' AS origin, 'modified' AS action, "
                            +cols+" FROM "+wcs+"."+table+"_update_diff "
                        "WHERE "+pkey+" = "+str(child)+" "
                        "AND "+branch+"_rev_end IS NULL" )
                    pcur.execute("INSERT INTO "+wcs+"."+table+"_cflt "
                        "SELECT "+str(cflt_id)+" AS conflict_id, "
                            "'theirs' AS origin, 'deleted' AS action, "
                            +cols+" FROM "+wcs+"."+table+"_update_diff "
                        "WHERE "+pkey+" = "+str(child)+" "
                        "AND "+branch+"_rev_end IS NOT NULL" )

            # create trigers such that on delete the conflict is resolved
            # if we delete 'theirs', we set their child to our fid
            # and their rev_end
            # if we delete 'mine'... well, we delete 'mine'

            pcur.execute("SELECT column_name "
                    "FROM information_schema.columns "
                    "WHERE table_schema = '"+wcs+"' "
                    "AND table_name = '"+table+"_diff'")
            cols = ""
            for col in pcur.fetchall():
                cols += quote_ident(col[0])+", "
            cols = cols[:-2] # remove last coma and space

            pcur.execute("CREATE OR REPLACE VIEW "
                +wcs+"."+table+"_conflicts AS SELECT * "
                "FROM  "+wcs+"."+table+"_cflt" )

            pcur.execute("CREATE OR REPLACE FUNCTION "
                +wcs+".delete_"+table+"_conflicts() RETURNS trigger AS $$\n"
                "BEGIN\n"
                    "DELETE FROM "+wcs+"."+table+"_diff "
                    "WHERE "+pkey+" = OLD."+pkey+" AND OLD.origin = 'mine';\n"

                    # we need to insert their parent to update it
                    # if it's not already there
                    "INSERT INTO "+wcs+"."+table+"_diff("+cols+") "
                    "SELECT "+cols+" FROM "+table_schema+"."+table+" "
                    "WHERE "+pkey+" = OLD."+branch+"_parent "
                    "AND OLD.origin = 'theirs' "
                    "AND (SELECT COUNT(*) FROM "+wcs+"."+table+"_diff "
                        "WHERE "+pkey+" =  OLD."+branch+"_parent ) = 0;\n"

                    "UPDATE "+wcs+"."+table+"_diff "
                    "SET "+branch+"_child = (SELECT "+pkey+" "
                                          "FROM "+wcs+"."+table+"_cflt "
                                          "WHERE origin = 'mine' "
                                          "AND conflict_id = OLD.conflict_id), "
                          +branch+"_rev_end = "+str(max_rev)+" "
                    "WHERE "+pkey+" = OLD."+pkey+" AND OLD.origin = 'theirs';\n"

                    "UPDATE "+wcs+"."+table+"_diff "
                    "SET "+branch+"_parent = OLD."+pkey+" "
                    "WHERE "+pkey+" = (SELECT "+pkey+" FROM "+wcs+"."+table+"_cflt "
                                    "WHERE origin = 'mine' "
                                    "AND conflict_id = OLD.conflict_id) "
                    "AND OLD.origin = 'theirs';\n"

                    "DELETE FROM "+wcs+"."+table+"_cflt "
                    "WHERE conflict_id = OLD.conflict_id;\n"
                    "RETURN NULL;\n"
                "END;\n"
            "$$ LANGUAGE plpgsql;")

            pcur.execute("DROP TRIGGER IF EXISTS "
                "delete_"+table+"_conflicts ON "+wcs+"."+table+"_conflicts ")
            pcur.execute("CREATE TRIGGER "
                "delete_"+table+"_conflicts "
                "INSTEAD OF DELETE ON "+wcs+"."+table+"_conflicts "
                "FOR EACH ROW "
                "EXECUTE PROCEDURE "+wcs+".delete_"+table+"_conflicts();")
            pcur.commit()

            pcur.execute("ALTER TABLE "+wcs+"."+table+"_cflt "
                "ADD CONSTRAINT "+table+"_"+branch+"conflicts_pk_pk "
                "PRIMARY KEY ("+pkey+")")

    pcur.commit()
    pcur.close()

def pg_commit(pg_conn_info, working_copy_schema, commit_msg):
    """merge modifications into database
    returns the number of updated layers"""
    wcs = working_copy_schema

    unresolved = pg_unresolved_conflicts(pg_conn_info, wcs)
    if unresolved:
        raise RuntimeError("There are unresolved conflicts in "+wcs+" "
            "for table(s) "+', '.join(unresolved) )

    late_by = pg_late(pg_conn_info, wcs)
    if late_by:
        raise RuntimeError("Working copy "+working_copy_schema+" "
            "is not up to date. It's late by "+str(late_by)+" commit(s).\n\n"
            "Please update before committing your modifications")

    # Better if we could have a QgsDataSourceURI.username()
    try :
        pg_username = pg_conn_info.split(' ')[3].replace("'","").split('=')[1]
    except (IndexError):
        pg_username = ''
    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT rev, branch, table_schema, table_name "
        "FROM "+wcs+".initial_revision")
    versioned_layers = pcur.fetchall()

    if not versioned_layers:
        raise RuntimeError("Cannot find a versioned layer in "+wcs)


    nb_of_updated_layer = 0
    next_rev = 0
    for [rev, branch, table_schema, table] in versioned_layers:
        if next_rev:
            assert( next_rev == rev + 1 )
        else: next_rev = rev + 1

        pkey = pg_pk( pcur, table_schema, table )
        history_columns = [pkey] + sum([
            [brch+'_rev_end', brch+'_rev_begin',
            brch+'_child', brch+'_parent' ] for brch in pg_branches( pcur, table_schema )],[])
        pcur.execute("SELECT column_name "
                "FROM information_schema.columns "
                "WHERE table_schema = '"+table_schema+"' "
                "AND table_name = '"+table+"'")
        cols = ""
        for [col] in pcur.fetchall():
            if col not in history_columns:
                cols = quote_ident(col)+", "+cols
        cols = cols[:-2] # remove last coma and space
        hcols = (pkey+", "+branch+"_rev_begin, "+branch+"_rev_end, "
                +branch+"_parent, "+branch+"_child")

        pcur.execute( "SELECT "+pkey+" FROM "+wcs+"."+table+"_diff")
        there_is_something_to_commit = pcur.fetchone()

        if not there_is_something_to_commit:
            print "nothing to commit for ", table
            continue
        nb_of_updated_layer += 1

        pcur.execute("SELECT rev FROM "+table_schema+".revisions "
            "WHERE rev = "+str(rev+1))
        if not pcur.fetchone():
            print "inserting rev ", str(rev+1)
            pcur.execute("INSERT INTO "+table_schema+".revisions "
                "(rev, commit_msg, branch, author) "
                "VALUES ("+str(rev+1)+", '"+escape_quote(commit_msg)+"', '"+branch+"',"
                "'"+os_info()+":"+get_username()+"."+pg_username+"')")

        # insert inserted and modified
        pcur.execute("INSERT INTO "+table_schema+"."+table+" "
            "("+cols+", "+hcols+") "
            "SELECT "+cols+", "+hcols+" FROM "+wcs+"."+table+"_diff "
            "WHERE "+branch+"_rev_begin = "+str(rev+1))

        # update deleted and modified
        pcur.execute("UPDATE "+table_schema+"."+table+" AS dest "
                "SET ("+branch+"_rev_end, "+branch+"_child)"
                    "=(src."+branch+"_rev_end, src."+branch+"_child) "
                "FROM "+wcs+"."+table+"_diff AS src "
                "WHERE dest."+pkey+" = src."+pkey+" "
                "AND src."+branch+"_rev_end = "+str(rev))

        print "truncate diff for ", table
        # clears the diff
        pcur.execute("TRUNCATE TABLE "+wcs+"."+table+"_diff CASCADE")
        #pcur.execute("DELETE FROM "+wcs+"."+table+"_diff_pkey")
        print "diff truncated for ", table

    if nb_of_updated_layer:
        for [rev, branch, table_schema, table] in versioned_layers:
            pkey = pg_pk( pcur, table_schema, table )
            pcur.execute("UPDATE "+wcs+".initial_revision "
                "SET (rev, max_pk) "
                "= ((SELECT MAX(rev) FROM "+table_schema+".revisions), "
                    "(SELECT MAX("+pkey+") FROM "+table_schema+"."+table+")) "
                "WHERE table_schema = '"+table_schema+"' "
                "AND table_name = '"+table+"' "
                "AND branch = '"+branch+"'")

    pcur.commit()
    pcur.close()
    return nb_of_updated_layer

def pg_unresolved_conflicts(pg_conn_info, working_copy_schema):
    """return a list of tables with unresolved conflicts"""
    found = []
    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='"+working_copy_schema+"' "
        "AND table_name LIKE '%_cflt'")
    for table_conflicts in pcur.fetchall():
        print 'table_conflicts:', table_conflicts[0]
        pcur.execute("SELECT * "
            "FROM "+working_copy_schema+"."+table_conflicts[0])
        if pcur.fetchone():
            found.append( table_conflicts[0][:-5] )
    pcur.commit()
    pcur.close()
    return found

def pg_late(pg_conn_info, working_copy_schema):
    """Return 0 if up to date, the number of commits in between otherwise"""
    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT rev, branch, table_schema "
        "FROM "+working_copy_schema+".initial_revision")
    versioned_layers = pcur.fetchall()
    if not versioned_layers:
        raise RuntimeError("Cannot find versioned layer in "
                +working_copy_schema)

    late_by = 0

    for [rev, branch, table_schema] in versioned_layers:
        pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions "
            "WHERE branch = '"+branch+"'")
        [max_rev] = pcur.fetchone()
        late_by = max(max_rev - rev, late_by)

    return late_by

def pg_revision( pg_conn_info, working_copy_schema ):
    """returns the revision the working copy was created from plus one"""
    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT rev "+ "FROM "+working_copy_schema+".initial_revision")
    rev = 0
    for [res] in pcur.fetchall():
        if rev :
            assert( res == rev )
        else :
            rev = res
    pcur.close()
    return rev + 1
