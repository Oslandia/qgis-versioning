import re
import os
import pwd
from pyspatialite import dbapi2
import psycopg2

class Db:
   def __init__(self, con, filename = ''):
       self.con = con
       if isinstance(con, dbapi2.Connection): self.db_type = 'sp : '
       else : self.db_type = 'pg : '
       self.cur = self.con.cursor()
       if filename : 
           self.log = open( filename, 'w' )
           self.log.write('-- openning connection\n')
       else :
           self.log = None
       self.begun = False
       self._verbose = False

   def verbose(self, v):
       self._verbose = v

   def execute(self, sql):
       if not self.begun:
           self.begun = True
           if self._verbose: print self.db_type, 'BEGIN;'
           if self.log : self.log.write( 'BEGIN;\n')
       if self._verbose: print self.db_type, sql, ';'
       if self.log : self.log.write(sql+';\n')
       self.cur.execute( sql )

   def fetchall(self):
       return self.cur.fetchall()

   def fetchone(self):
       return self.cur.fetchone()

   def commit(self):
       if self._verbose: print self.db_type, 'END;'
       if self.log : self.log.write('END;\n')
       self.begun = False;
       self.con.commit()

   def close(self):
       if self.begun : 
           if self._verbose: print self.db_type, 'END;'
           if self.log : self.log.write('END;\n')
       if self.log : self.log.write('-- closing connection\n')
       self.con.close()


def escapeQuotes(s):
    return str.replace(str(s),"'","''");

def get_username():
    return pwd.getpwuid( os.getuid() )[ 0 ]

def unresolvedConflicts(sqlite_filename):
    """return a list of tables with unresolved conflicts"""
    found = []
    scur = Db(dbapi2.connect(sqlite_filename))
    scur.execute("SELECT tbl_name FROM sqlite_master WHERE type='table' AND tbl_name LIKE '%_conflicts'")
    for table_conflicts in scur.fetchall():
        print 'table_conflicts:',table_conflicts[0]
        scur.execute("SELECT * FROM "+table_conflicts[0])
        if scur.fetchone():
            found.append( table_conflicts[0][:-10] )
    scur.commit()
    scur.close()
    return found

def checkout(pg_conn_info, pg_table_names, sqlite_filename):
    """create working copy from versioned database tables
    pg_table_names must be complete schema.table names
    the schema name must end with _branch_rev_head
    the file sqlite_filename must not exists
    the views and trigger for local edition will be created
    along with the tables and triggers for conflict resolution"""

    if os.path.isfile(sqlite_filename): raise RuntimeError("File "+sqlite_filename+" already exists")
    for pg_table_name in pg_table_names:
        [schema, table] = pg_table_name.split('.')
        if not ( schema and table and schema[-9:] == "_rev_head"): raise RuntimeError("Schema names must end with suffix _branch_rev_head")

    pcur = Db(psycopg2.connect(pg_conn_info))

    first_table = True
    for pg_table_name in pg_table_names:
        [schema, table] = pg_table_name.split('.')
        [schema, sep, branch] = schema[:-9].rpartition('_')

        # fetch the current rev
        pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
        current_rev = int(pcur.fetchone()[0])

        # max hid for this table
        pcur.execute("SELECT MAX(hid) FROM "+schema+"."+table)
        max_pg_hid = int(pcur.fetchone()[0])

        # use ogr2ogr to create spatialite db
        if first_table:
            first_table = False
            cmd = ['ogr2ogr', '-preserve_fid', '-f', 'SQLite', '-dsco', 'SPATIALITE=yes', sqlite_filename, 'PG:"'+pg_conn_info+' active_schema='+schema+'"', table]
            print ' '.join(cmd)
            os.system(' '.join(cmd))

            # save target revision in a table
            scur = Db(dbapi2.connect(sqlite_filename))
            scur.execute("CREATE TABLE initial_revision AS SELECT "+
                    str(current_rev)+" AS rev, '"+
                    branch+"' AS branch, '"+
                    schema+"' AS table_schema, '"+
                    table+"' AS table_name, '"+
                    escapeQuotes(pg_conn_info)+"' AS conn_info, "+
                    str(max_pg_hid)+" AS max_hid")
            scur.commit()
            scur.close()
            
        else:
            cmd = ['ogr2ogr', '-preserve_fid', '-f', 'SQLite', '-update', sqlite_filename, 'PG:"'+pg_conn_info+' active_schema='+schema+'"', table]
            print ' '.join(cmd)
            os.system(' '.join(cmd))

            # save target revision in a table if not in there
            scur = Db(dbapi2.connect(sqlite_filename))
            scur.execute("INSERT INTO initial_revision(rev, branch, table_schema, table_name, conn_info, max_hid) VALUES ("+str(current_rev)+", '"+branch+"', '"+schema+"', '"+table+"', '"+escapeQuotes(pg_conn_info)+"', "+str(max_pg_hid)+")" )
            scur.commit()
            scur.close()

        scur = Db(dbapi2.connect(sqlite_filename))

        # create views and triggers in spatilite db
        scur.execute("PRAGMA table_info("+table+")")
        cols = ""
        newcols = ""
        hcols = ['OGC_FID', branch+'_rev_begin', branch+'_rev_end', branch+'_parent', branch+'_child']
        for r in scur.fetchall():
            if r[1] not in hcols : 
                cols += r[1] + ", "
                newcols += "new."+r[1]+", "
        cols = cols[:-2]
        newcols = newcols[:-2] # remove last coma

        scur.execute( "CREATE VIEW "+table+"_view "+"AS SELECT ROWID AS ROWID, OGC_FID, "+cols+" FROM "+table+" WHERE "+branch+"_rev_end IS NULL")

        max_fid_sub = "( SELECT MAX(max_fid) FROM ( SELECT MAX(OGC_FID) AS max_fid FROM "+table+" UNION SELECT max_hid AS max_fid FROM initial_revision WHERE table_name = '"+table+"') )"
        current_rev_sub = "(SELECT rev FROM initial_revision WHERE table_name = '"+table+"')"

        scur.execute("DELETE FROM views_geometry_columns WHERE f_table_name = '"+table+"_conflicts'")
        scur.execute("INSERT INTO views_geometry_columns "+"(view_name, view_geometry, view_rowid, f_table_name, f_geometry_column) "+"VALUES"+"('"+table+"_view', 'GEOMETRY', 'ROWID', '"+table+"', 'GEOMETRY')")
         
        # when we edit something old, we insert and update parent
        scur.execute("CREATE TRIGGER update_old_"+table+" INSTEAD OF UPDATE ON "+table+"_view "+
              "WHEN (SELECT COUNT(*) FROM "+table+" WHERE OGC_FID = new.OGC_FID AND ("+branch+"_rev_begin <= "+current_rev_sub+" ) ) \n"+
              "BEGIN\n"+
                "INSERT INTO "+table+" "
                "(OGC_FID, "+cols+", "+branch+"_rev_begin, "+branch+"_parent) "+
                "VALUES "
                "("+max_fid_sub+"+1, "+newcols+", "+current_rev_sub+"+1, old.OGC_FID);\n"+
                "UPDATE "+table+" SET "+branch+"_rev_end = "+current_rev_sub+", "+branch+"_child = "+max_fid_sub+" WHERE OGC_FID = old.OGC_FID;\n"+
              "END")
        # when we edit something new, we just update
        scur.execute("CREATE TRIGGER update_new_"+table+" INSTEAD OF UPDATE ON "+table+"_view "+
              "WHEN (SELECT COUNT(*) FROM "+table+" WHERE OGC_FID = new.OGC_FID AND ("+branch+"_rev_begin > "+current_rev_sub+" ) ) \n"+
              "BEGIN\n"+
                "REPLACE INTO "+table+" "
                "(OGC_FID, "+cols+", "+branch+"_rev_begin, "+branch+"_parent) "+
                "VALUES "
                "(new.OGC_FID, "+newcols+", "+current_rev_sub+"+1, (SELECT "+branch+"_parent FROM "+table+" WHERE OGC_FID = new.OGC_FID));\n"+
              "END")

        scur.execute("CREATE TRIGGER insert_"+table+" INSTEAD OF INSERT ON "+table+"_view\n"+
              "BEGIN\n"+
                "INSERT INTO "+table+" "+ 
                "(OGC_FID, "+cols+", "+branch+"_rev_begin) "+
                "VALUES "+
                "("+max_fid_sub+"+1, "+newcols+", "+current_rev_sub+"+1);\n"+
              "END")

        scur.execute("CREATE TRIGGER delete_"+table+" INSTEAD OF DELETE ON "+table+"_view\n"+
              "BEGIN\n"+
              # update it if its old
                "UPDATE "+table+" "+
                    "SET "+branch+"_rev_end = "+current_rev_sub+" "+
                    "WHERE OGC_FID = old.OGC_FID "+
                    "AND "+branch+"_rev_begin < "+current_rev_sub+"+1;\n"+
              # delete it if its new and remove it from child
                "UPDATE "+table+" "+
                    "SET "+branch+"_child = NULL "+
                    "WHERE "+branch+"_child = old.OGC_FID "+
                    "AND "+branch+"_rev_begin = "+current_rev_sub+"+1;\n"+
                "DELETE FROM "+table+" "+
                    "WHERE OGC_FID = old.OGC_FID "+
                    "AND "+branch+"_rev_begin = "+current_rev_sub+"+1;\n"+
              "END")
        
        scur.commit()
        scur.close()
    pcur.close()

def update(sqlite_filename):
    """merge modifiactions since last update into working copy"""
    print "update"
    if unresolvedConflicts(sqlite_filename): raise RuntimeError("There are unresolved conflicts in "+sqlite_filename)
    # get the target revision from the spatialite db
    # create the diff in postgres
    # load the diff in spatialite
    # detect conflicts and create conflict layers
    # merge changes and update target_revision
    # delete diff
    
    scur = Db(dbapi2.connect(sqlite_filename),'update_spatialite_log.sql')
    scur.execute("SELECT rev, branch, table_schema, conn_info, table_name, max_hid "+
        "FROM initial_revision")
    versioned_layers = scur.fetchall()

    for [rev, branch, table_schema, conn_info, table, current_max_hid] in versioned_layers:
        pcur = Db(psycopg2.connect(conn_info))
        pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions WHERE branch = '"+branch+"'")
        [max_rev] = pcur.fetchone()
        if max_rev == rev: 
            print "Nothing new in branch "+branch+" in "+table_schema+"."+table+" since last update"
            pcur.close()
            continue

        # get the max hid 
        pcur.execute("SELECT MAX(hid) FROM "+table_schema+"."+table)
        [max_pg_hid] = pcur.fetchone()

        # create the diff
        diff_schema = table_schema+"_"+branch+"_"+str(rev)+"_to_"+str(max_rev)+"_diff"
        pcur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"+diff_schema+"'")
        if not pcur.fetchone(): pcur.execute("CREATE SCHEMA "+diff_schema)

        # TODO fix this to remove others branches from cols
        pcur.execute("SELECT column_name "+
                "FROM information_schema.columns "+
                "WHERE table_schema = '"+table_schema+"' AND table_name = '"+table+"'")
        cols = ""
        for c in pcur.fetchall(): 
            if c[0] != "geom": cols += c[0]+", "
        cols = cols[:-2] # remove last coma and space

        pcur.execute("SELECT srid, type "+
            "FROM geometry_columns "+
            "WHERE f_table_schema = '"+table_schema+"' AND f_table_name ='"+table+"' AND f_geometry_column = 'geom'")
        [srid, geom_type] = pcur.fetchone()
        pcur.execute( "DROP TABLE IF EXISTS "+diff_schema+"."+table+"_diff")
        pcur.execute( "CREATE TABLE "+diff_schema+"."+table+"_diff AS "+
                "SELECT "+cols+", geom::geometry('"+geom_type+"', "+str(srid)+") AS geom "+
                "FROM "+table_schema+"."+table+" "+
                "WHERE "+branch+"_rev_end = "+str(rev)+" OR "+branch+"_rev_begin > "+str(rev))
        pcur.execute( "ALTER TABLE "+diff_schema+"."+table+"_diff "+
                "ADD CONSTRAINT "+table+"_"+branch+"_hid_pk PRIMARY KEY (hid)") 
        pcur.commit()

        scur.execute("DROP TABLE IF EXISTS "+table+"_diff")
        scur.execute("DROP TABLE IF EXISTS idx_"+table+"_diff_GEOMETRY")
        scur.execute("DELETE FROM geometry_columns WHERE f_table_name = '"+table+"_diff'")
        scur.commit()

        # import the diff to spatialite
        cmd = ['ogr2ogr', '-preserve_fid', '-f', 'SQLite', '-update', sqlite_filename, 'PG:"'+conn_info+' active_schema='+diff_schema+'"', table+"_diff"]
        print ' '.join(cmd)
        os.system(' '.join(cmd))

        # cleanup in postgis
        pcur.execute("DROP SCHEMA "+diff_schema+" CASCADE")
        pcur.commit()
        pcur.close()

        scur.execute("PRAGMA table_info("+table+")")
        cols = ""
        for c in scur.fetchall(): cols += c[1]+", "
        cols = cols[:-2] # remove last coma and space

        # update the initial revision 
        scur.execute("UPDATE initial_revision SET rev = "+str(max_rev)+", max_hid = "+str(max_pg_hid)+" WHERE table_name = '"+table+"'")
        
        scur.execute("UPDATE "+table+" "+
                "SET "+branch+"_rev_end = "+str(max_rev)+" "+
                "WHERE "+branch+"_rev_end = "+str(rev))
        scur.execute("UPDATE "+table+" "+
                "SET "+branch+"_rev_begin = "+str(max_rev+1)+" "+
                "WHERE "+branch+"_rev_begin = "+str(rev+1))
        
        # we cannot add constrain to the spatialite db in order to have spatialite
        # update parent and child when we bump inserted hid above the max hid in the diff
        # we must do this manually
        bump = max_pg_hid - current_max_hid
        assert( bump >= 0) 
        # now bump the hids of inserted rows in working copy
        # note that to do that, we need to set a negative value because 
        # the UPDATE is not implemented correctly according to:
        # http://stackoverflow.com/questions/19381350/simulate-order-by-in-sqlite-update-to-handle-uniqueness-constraint
        scur.execute("UPDATE "+table+" "+
                "SET OGC_FID = -OGC_FID  "+
                "WHERE "+branch+"_rev_begin = "+str(max_rev+1))
        scur.execute("UPDATE "+table+" SET OGC_FID = "+str(bump)+"-OGC_FID WHERE OGC_FID < 0")
        # and bump the hid in the child field
        # not that we don't care for nulls since adding something to null is null
        scur.execute("UPDATE "+table+" "+
                "SET "+branch+"_child = "+branch+"_child  + "+str(bump)+" "+
                "WHERE "+branch+"_rev_end = "+str(max_rev))

        # detect conflicts: conflict occur if two lines with the same hid have
        # been modified (i.e. have a non null child) or one has been removed
        # and the other modified
        scur.execute("DROP VIEW  IF EXISTS "+table+"_conflicts_ogc_fid")
        scur.execute("CREATE VIEW "+table+"_conflicts_ogc_fid AS "+
            "SELECT DISTINCT sl.OGC_FID as conflict_deleted_fid "+
            "FROM "+table+" AS sl, "+table+"_diff AS pg "+
            "WHERE sl.OGC_FID = pg.OGC_FID "+
                "AND sl."+branch+"_child != pg."+branch+"_child")
        scur.execute("SELECT conflict_deleted_fid FROM  "+table+"_conflicts_ogc_fid" )
        if scur.fetchone():
            print "there are conflicts"
            # add layer for conflicts
            scur.execute("DROP TABLE IF EXISTS "+table+"_conflicts ")
            scur.execute("CREATE TABLE "+table+"_conflicts AS "+
                # insert new features from mine
                "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, 'modified' AS action, "+cols+" "+
                "FROM "+table+", "+table+"_conflicts_ogc_fid AS cflt "+
                "WHERE OGC_FID = (SELECT "+branch+"_child FROM "+table+" "+
                                     "WHERE OGC_FID = conflict_deleted_fid) "+
                "UNION ALL "
                # insert new features from theirs
                "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, 'modified' AS action, "+cols+" "+
                "FROM "+table+"_diff "+", "+table+"_conflicts_ogc_fid AS cflt "+
                "WHERE OGC_FID = (SELECT "+branch+"_child FROM "+table+"_diff "+
                                     "WHERE OGC_FID = conflict_deleted_fid) "+
                 # insert deleted features from mine
                "UNION ALL "+
                "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, 'deleted' AS action, "+cols+" "+
                "FROM "+table+", "+table+"_conflicts_ogc_fid AS cflt "+
                "WHERE OGC_FID = conflict_deleted_fid AND "+branch+"_child IS NULL "+
                 # insert deleted features from theirs
                "UNION ALL "+
                "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, 'deleted' AS action, "+cols+" "+
                "FROM "+table+"_diff, "+table+"_conflicts_ogc_fid AS cflt "+
                "WHERE OGC_FID = conflict_deleted_fid AND "+branch+"_child IS NULL" )

            # identify conflicts for deleted 
            scur.execute("UPDATE "+table+"_conflicts "+ "SET conflict_id = OGC_FID "+ "WHERE action = 'deleted'")

            # now follow child if any for 'theirs' 'modified' since several edition could be made
            # we want the very last child
            while True:
                scur.execute("SELECT conflict_id, OGC_FID, "+branch+"_child FROM "+table+"_conflicts WHERE origin='theirs' AND action='modified' AND "+branch+"_child IS NOT NULL");
                r = scur.fetchall()
                if not r : break
                # replaces each entries by it's child
                for [cflt_id, fid, child] in r:
                    scur.execute("DELETE FROM "+table+"_conflicts WHERE OGC_FID = "+str(fid))
                    scur.execute("INSERT INTO "+table+"_conflicts "+
                        "SELECT "+str(cflt_id)+" AS conflict_id, 'theirs' AS origin, 'modified' AS action, "+cols+" FROM "+table+"_diff "+
                        "WHERE OGC_FID = "+str(child)+" AND "+branch+"_rev_end IS NULL" );
                    scur.execute("INSERT INTO "+table+"_conflicts "+
                        "SELECT "+str(cflt_id)+" AS conflict_id, 'theirs' AS origin, 'deleted' AS action, "+cols+" FROM "+table+"_diff "+
                        "WHERE OGC_FID = "+str(child)+" AND "+branch+"_rev_end IS NOT NULL" );

            scur.execute("DELETE FROM geometry_columns WHERE f_table_name = '"+table+"_conflicts'")
            scur.execute("SELECT RecoverGeometryColumn('"+table+"_conflicts', 'GEOMETRY', (SELECT srid FROM geometry_columns WHERE f_table_name='"+table+"'), (SELECT type FROM geometry_columns WHERE f_table_name='"+table+"'), 'XY')")
            
            scur.execute("CREATE UNIQUE INDEX IF NOT EXISTS "+table+"_conflicts_idx ON "+table+"_conflicts(OGC_FID)")

            # create trigers such that on delete the conflict is resolved
            # if we delete 'theirs', we set their child to our fid and their rev_end
            # if we delete 'mine'... well, we delete 'mine'

            scur.execute("DROP TRIGGER IF EXISTS delete_"+table+"_conflicts")
            scur.execute("CREATE TRIGGER delete_"+table+"_conflicts AFTER DELETE ON "+table+"_conflicts\n"+
                "BEGIN\n"+
                    "DELETE FROM "+table+" "+
                    "WHERE OGC_FID = old.OGC_FID AND old.origin = 'mine';\n"+

                    "UPDATE "+table+" "+
                    "SET "+branch+"_child = (SELECT OGC_FID FROM "+table+"_conflicts WHERE origin = 'mine' AND conflict_id = old.conflict_id), "+branch+"_rev_end = "+str(max_rev)+" "
                    "WHERE OGC_FID = old.OGC_FID AND old.origin = 'theirs';\n"+

                    "UPDATE "+table+" "+
                    "SET "+branch+"_parent = old.OGC_FID "+
                    "WHERE OGC_FID = (SELECT OGC_FID FROM "+table+"_conflicts WHERE origin = 'mine' AND conflict_id = old.conflict_id) AND old.origin = 'theirs';\n"+

                    "DELETE FROM "+table+"_conflicts "+
                    "WHERE conflict_id = old.conflict_id;\n"+
                "END")

            scur.commit()

        scur.execute("CREATE UNIQUE INDEX IF NOT EXISTS "+table+"_diff_idx ON "+table+"_diff(OGC_FID)")
        # insert and replace all in diff
        scur.execute("INSERT OR REPLACE INTO "+table+" ("+cols+") "+
            "SELECT "+cols+" FROM "+table+"_diff")

    scur.commit()
    scur.close()

def late(sqlite_filename):
    """Return 0 if up to date, the number of commits in between otherwize"""
    scur = Db(dbapi2.connect(sqlite_filename))
    scur.execute("SELECT rev, branch, table_schema, conn_info, table_name "+
        "FROM initial_revision")
    versioned_layers = scur.fetchall()
    if not versioned_layers: raise RuntimeError("Cannot find versioned layer in "+sqlite_filename)

    lateBy = 0;

    for [rev, branch, table_schema, conn_info, table] in versioned_layers:
        pcur = Db(psycopg2.connect(conn_info))
        pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions WHERE branch = '"+branch+"'")
        [max_rev] = pcur.fetchone()
        lateBy = max(max_rev - rev, lateBy)

    return lateBy

def revision( sqlite_filename ):
    """returns the revision the working copy was created from plus one"""
    scur = Db(dbapi2.connect(sqlite_filename))
    scur.execute("SELECT rev "+ "FROM initial_revision")
    rev = scur.fetchall()
    revision = 0
    for [r] in rev:
        if revision : assert( r == revision )
        else : revision = r
    scur.close()
    return revision + 1

def commit(sqlite_filename, commit_msg):
    """merge modifications into database
    returns the number of updated layers"""
    # get the target revision from the spatialite db
    # create the diff in postgres
    # load the diff in spatialite
    # detect conflicts
    # merge changes and update target_revision
    # delete diff

    unresolved = unresolvedConflicts(sqlite_filename)
    if unresolved: raise RuntimeError("There are unresolved conflicts in "+sqlite_filename+" for table(s) "+', '.join(unresolved) )

    lateBy = late(sqlite_filename)
    if lateBy:  raise RuntimeError("Working copy "+sqlite_filename+" is not up to date. It's late by "+str(lateBy)+" commit(s).\n\nPlease update before commiting your modifications")

    scur = Db(dbapi2.connect(sqlite_filename))
    scur.execute("SELECT rev, branch, table_schema, conn_info, table_name "+
        "FROM initial_revision")
    versioned_layers = scur.fetchall()

    if not versioned_layers: raise RuntimeError("Cannot find a versioned layer in "+sqlite_filename)

    schema_list={} # for final cleanup
    nb_of_updated_layer = 0
    next_rev = 0;
    for [rev, branch, table_schema, conn_info, table] in versioned_layers:
        diff_schema = table_schema+"_"+branch+"_"+str(rev)+"_to_"+str(rev+1)+"_diff"
        
        if next_rev: assert( next_rev == rev + 1 )
        else: next_rev = rev + 1

        scur.execute( "DROP TABLE IF EXISTS "+table+"_diff")

        # note, creating the diff table dirrectly with CREATE TABLE... AS SELECT won't work
        # types get fubared in the process
        # therefore we copy the creation statement from spatialite master and change the
        # table name ta obtain a similar table, we add the geometry column 
        # to geometry_columns manually and we insert the diffs
        scur.execute("SELECT sql FROM sqlite_master WHERE tbl_name = '"+table+"' AND type = 'table'")
        [sql] = scur.fetchone()
        sql = unicode.replace(sql,table,table+"_diff",1)
        scur.execute(sql)
        scur.execute("DELETE FROM geometry_columns WHERE f_table_name = '"+table+"_diff'")
        scur.execute("INSERT INTO geometry_columns SELECT '"+table+"_diff', 'GEOMETRY', type, coord_dimension, srid, spatial_index_enabled FROM geometry_columns WHERE f_table_name = '"+table+"'")
        scur.execute( "INSERT INTO "+table+"_diff "+
                "SELECT * "+
                "FROM "+table+" "+
                "WHERE "+branch+"_rev_end = "+str(rev)+" OR "+branch+"_rev_begin > "+str(rev))
        scur.execute( "SELECT OGC_FID FROM "+table+"_diff")
        there_is_something_to_commit = scur.fetchone()
        print "there_is_something_to_commit ", there_is_something_to_commit
        scur.commit()

        pcur = Db(psycopg2.connect(conn_info))

        # import layers in postgis schema
        pcur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"+diff_schema+"'")
        if not pcur.fetchone():
            schema_list[diff_schema] = conn_info
            pcur.execute("CREATE SCHEMA "+diff_schema)
        pcur.execute( "DROP TABLE IF EXISTS "+diff_schema+"."+table+"_diff")
        pcur.commit()
        cmd = ['ogr2ogr', '-preserve_fid', '-f', 'PostgreSQL', 'PG:"'+conn_info+' active_schema='+diff_schema+'"', '-lco', 'GEOMETRY_NAME=geom', '-lco', 'FID=hid', sqlite_filename, table+"_diff"]
        print ' '.join(cmd)
        os.system(' '.join(cmd))

        # remove dif table and geometry column
        scur.execute("DELETE FROM geometry_columns WHERE f_table_name = '"+table+"_diff'")
        scur.execute("DROP TABLE "+table+"_diff")


        if not there_is_something_to_commit: 
            print "nothing to commit for ", table
            pcur.close()
            continue

        nb_of_updated_layer += 1

        pcur.execute("SELECT rev FROM "+table_schema+".revisions WHERE rev = "+str(rev+1))
        if not pcur.fetchone():
            print "inserting rev ", str(rev+1)
            pcur.execute("INSERT INTO "+table_schema+".revisions (rev, commit_msg, branch, author) VALUES ("+str(rev+1)+", '"+commit_msg+"', '"+branch+"', '"+get_username()+"')")

        # TODO fix this to avoid hcols from other branches
        pcur.execute("SELECT column_name "+
                "FROM information_schema.columns "+
                "WHERE table_schema = '"+table_schema+"' AND table_name = '"+table+"'")
        cols = ""
        for c in pcur.fetchall(): cols += c[0]+", "
        cols = cols[:-2] # remove last coma and space
        # insert inserted and modified
        pcur.execute("INSERT INTO "+table_schema+"."+table+" ("+cols+") "+
            "SELECT "+cols+" FROM "+diff_schema+"."+table+"_diff "+
            "WHERE "+branch+"_rev_begin = "+str(rev+1))

        # update deleted and modified 
        pcur.execute("UPDATE "+table_schema+"."+table+" AS dest "+
                "SET ("+branch+"_rev_end, "+branch+"_child)=(src."+branch+"_rev_end, src."+branch+"_child) "+
                "FROM "+diff_schema+"."+table+"_diff AS src "+
                "WHERE dest.hid = src.hid AND src."+branch+"_rev_end = "+str(rev))
        pcur.commit()
        pcur.close()

        scur.commit()

    if nb_of_updated_layer:
        scur.execute("UPDATE initial_revision SET rev = rev+1 WHERE table_schema = '"+table_schema+"' AND branch = '"+branch+"'")
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
    pcur = Db(psycopg2.connect(pg_conn_info))

    pcur.execute("CREATE TABLE "+schema+".revisions ("+
        "rev serial PRIMARY KEY, "+
        "commit_msg varchar, "+
        "branch varchar DEFAULT 'trunk', "+
        "date timestamp DEFAULT current_timestamp, "+
        "author varchar)")
    pcur.commit()
    pcur.close()
    add_branch( pg_conn_info, schema, 'trunk', 'initial commit' )

def add_branch( pg_conn_info, schema, branch, commit_msg, base_branch='trunk', base_rev='head' ):
    pcur = Db(psycopg2.connect(pg_conn_info))

    # check that branch doesn't exist and that base_branch exists and that base_rev is ok
    pcur.execute("SELECT * FROM "+schema+".revisions WHERE branch = '"+branch+"'")
    if pcur.fetchone():
        pcur.close()
        raise RuntimeError("Branch "+branch+" already exists")
    pcur.execute("SELECT * FROM "+schema+".revisions WHERE branch = '"+base_branch+"'")
    if branch != 'trunk' and not pcur.fetchone(): 
        pcur.close()
        raise RuntimeError("Base branch "+base_branch+" doesn't exist")
    pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
    [max_rev] = pcur.fetchone()
    if base_rev != 'head' and (int(base_rev) > max_rev or int(base_rev) <= 0): 
        pcur.close()
        raise RuntimeError("Revision "+str(base_rev)+" doesn't exist")

    pcur.execute("INSERT INTO "+schema+".revisions(branch, commit_msg ) VALUES ('"+branch+"', '"+commit_msg+"')")
    pcur.execute("CREATE SCHEMA "+schema+"_"+branch+"_rev_head")
   
    history_columns = [] 
    pcur.execute("SELECT DISTINCT branch FROM "+schema+".revisions")
    for [b] in pcur.fetchall():
        history_columns.extend([b+'_rev_end', b+'_rev_begin', b+'_child', b+'_parent'])

    security = ' WITH (security_barrier)'
    pcur.execute("SELECT version()")
    [version] = pcur.fetchone()
    m = re.match( '^PostgreSQL (\d+)\.(\d+)\.(\d+) ', version )
    if m and int(m.group(1)) <= 9 and int(m.group(2)) <= 2 : security = ''

    pcur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '"+schema+"'")
    for [table] in pcur.fetchall():
        if table == 'revisions': continue

        if branch == 'trunk': # initial versioning
            pcur.execute("ALTER TABLE "+schema+"."+table+" ADD COLUMN hid serial PRIMARY KEY")

        pcur.execute("ALTER TABLE "+schema+"."+table+" "+
            "ADD COLUMN "+branch+"_rev_begin integer REFERENCES "+schema+".revisions(rev), "+
            "ADD COLUMN "+branch+"_rev_end   integer REFERENCES "+schema+".revisions(rev), "+
            "ADD COLUMN "+branch+"_parent    integer REFERENCES "+schema+"."+table+"(hid),"+
            "ADD COLUMN "+branch+"_child     integer REFERENCES "+schema+"."+table+"(hid)")
        if branch == 'trunk': # initial versioning
            pcur.execute("UPDATE "+schema+"."+table+" SET "+branch+"_rev_begin = (SELECT MAX(rev) FROM "+schema+".revisions)")
        elif base_rev == "head":
            pcur.execute("UPDATE "+schema+"."+table+" "+
                    "SET "+branch+"_rev_begin = (SELECT MAX(rev) FROM "+schema+".revisions "+
                    "WHERE "+base_branch+"_rev_end IS NULL AND "+base_branch+"_rev_begin IS NOT NULL)")
        else:
            pcur.execute("UPDATE "+schema+"."+table+" "+
                    "SET "+branch+"_rev_begin = (SELECT MAX(rev) FROM "+schema+".revisions)"+
                    "WHERE ("+base_branch+"_rev_end IS NULL OR "+base_branch+"_rev_end > "+base_rev+") AND "+base_branch+"_rev_begin IS NOT NULL")

        pcur.execute("SELECT column_name "+
                "FROM information_schema.columns "+
                "WHERE table_schema = '"+schema+"' AND table_name = '"+table+"'")
        cols = ""
        for [c] in pcur.fetchall(): 
            if c not in history_columns: cols = c+", "+cols
        cols = cols[:-2] # remove last coma and space
        pcur.execute("CREATE VIEW "+schema+"_"+branch+"_rev_head."+table+" "+security+" AS "+
            "SELECT "+cols+" FROM "+schema+"."+table+" "+
            "WHERE "+branch+"_rev_end IS NULL AND "+branch+"_rev_begin IS NOT NULL")
    pcur.commit()
    pcur.close()

def add_revision_view(pg_conn_info, schema, branch, rev):
    pcur = Db(psycopg2.connect(pg_conn_info))

    pcur.execute("SELECT * FROM "+schema+".revisions WHERE branch = '"+branch+"'")
    if not pcur.fetchone(): 
        pcur.close()
        raise RuntimeError("Branch "+branch+" doesn't exist")
    pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
    [max_rev] = pcur.fetchone()
    if int(rev) > max_rev or int(rev) <= 0: 
        pcur.close()
        raise RuntimeError("Revision "+str(rev)+" doesn't exist")

    history_columns = [] 
    pcur.execute("SELECT DISTINCT branch FROM "+schema+".revisions")
    for [b] in pcur.fetchall():
        history_columns.extend([b+'_rev_end', b+'_rev_begin', b+'_child', b+'_parent'])

    rev_schema = schema+"_"+branch+"_rev_"+str(rev)

    pcur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"+rev_schema+"'")
    if pcur.fetchone():
        print rev_schema, ' already exists'
        return

    security = ' WITH (security_barrier)'
    pcur.execute("SELECT version()")
    [version] = pcur.fetchone()
    m = re.match( '^PostgreSQL (\d+)\.(\d+)\.(\d+) ', version )
    if m and int(m.group(1)) <= 9 and int(m.group(2)) <= 2 : security = ''

    pcur.execute("CREATE SCHEMA "+rev_schema)

    pcur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '"+schema+"'")

    for [table] in pcur.fetchall():
        if table == 'revisions': continue
        pcur.execute("SELECT column_name "+
                "FROM information_schema.columns "+
                "WHERE table_schema = '"+schema+"' AND table_name = '"+table+"'")
        cols = ""
        for [c] in pcur.fetchall(): 
            if c not in history_columns: cols = c+", "+cols
        cols = cols[:-2] # remove last coma and space
        pcur.execute("CREATE VIEW "+rev_schema+"."+table+" "+security+" AS "+
           "SELECT "+cols+" FROM "+schema+"."+table+" "+
           "WHERE ( "+branch+"_rev_end IS NULL OR "+branch+"_rev_end >= "+str(rev)+" ) AND "+branch+"_rev_begin <= "+str(rev))
          
    pcur.commit()
    pcur.close()

def branches(pg_conn_info, schema):
    """returns a list of branches for this schema"""
    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT DISTINCT branch FROM "+schema+".revisions")
    branches = []
    for [b] in pcur.fetchall(): branches.append(b)
    pcur.close()
    return branches

def revisions(pg_conn_info, schema):
    """returns a list of revisions for this schema"""
    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT rev FROM "+schema+".revisions")
    revs = []
    for [r] in pcur.fetchall(): revs.append(r)
    pcur.close()
    return revs


# functions checkout, update and commit for a posgres working copy
# we don't want to duplicate data
# we need the initial_revision table all the same
# for each table we need a diff and a view and triggers

def pg_checkout(pg_conn_info, pg_table_names, working_copy_schema):
    """create posgress working copy from versioned database tables
    pg_table_names must be complete schema.table names
    the schema name must end with _branch_rev_head
    the working_copy_schema must not exists
    the views and trigger for local edition will be created
    along with the tables and triggers for conflict resolution"""
    pcur = Db(psycopg2.connect(pg_conn_info))
    wcs = working_copy_schema
    pcur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"+wcs+"'")
    if pcur.fetchone(): raise RuntimeError("Schema "+wcs+" already exists")

    for pg_table_name in pg_table_names:
        [schema, table] = pg_table_name.split('.')
        if not ( schema and table and schema[-9:] == "_rev_head"): raise RuntimeError("Schema names must end with suffix _branch_rev_head")


    pcur.execute("CREATE SCHEMA "+wcs)
    
    first_table = True
    for pg_table_name in pg_table_names:
        [schema, table] = pg_table_name.split('.')
        [schema, sep, branch] = schema[:-9].rpartition('_')

        history_columns = ['hid'] 
        pcur.execute("SELECT DISTINCT branch FROM "+schema+".revisions")
        for [b] in pcur.fetchall():
            history_columns.extend([b+'_rev_end', b+'_rev_begin', b+'_child', b+'_parent'])

        # fetch the current rev
        pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
        current_rev = int(pcur.fetchone()[0])

        # max hid for this table
        pcur.execute("SELECT MAX(hid) FROM "+schema+"."+table)
        max_pg_hid = int(pcur.fetchone()[0])
        if first_table:
            first_table = False
            pcur.execute("CREATE TABLE "+wcs+".initial_revision AS SELECT "+
                    str(current_rev)+" AS rev, '"+
                    branch+"'::varchar AS branch, '"+
                    schema+"'::varchar AS table_schema, '"+
                    table+"'::varchar AS table_name, "+
                    str(max_pg_hid)+" AS max_hid")
        else:
            pcur.execute("INSERT INTO "+wcs+".initial_revision(rev, branch, table_schema, table_name, max_hid) VALUES ("+str(current_rev)+", '"+branch+"', '"+schema+"', '"+table+"', "+str(max_pg_hid)+")" )

        # create diff, views and triggers
        pcur.execute("SELECT column_name "+
                "FROM information_schema.columns "+
                "WHERE table_schema = '"+schema+"' AND table_name = '"+table+"'")
        cols = ""
        newcols = ""
        for [c] in pcur.fetchall(): 
            if c not in history_columns:
                cols = c+", "+cols
                newcols = "new."+c+", "+newcols
        cols = cols[:-2] 
        newcols = newcols[:-2] # remove last coma and space
        hcols = "hid, "+branch+"_rev_begin, "+branch+"_rev_end, "+branch+"_parent, "+branch+"_child"

        pcur.execute("CREATE TABLE "+wcs+"."+table+"_diff "+
                "AS SELECT "+cols+" FROM "+schema+"."+table+" WHERE False")

        pcur.execute("ALTER TABLE "+wcs+"."+table+"_diff "+
            "ADD COLUMN hid integer PRIMARY KEY, "+
            "ADD COLUMN "+branch+"_rev_begin integer, "+
            "ADD COLUMN "+branch+"_rev_end   integer, "+
            "ADD COLUMN "+branch+"_parent    integer REFERENCES "+wcs+"."+table+"_diff(hid),"+
            "ADD COLUMN "+branch+"_child     integer REFERENCES "+wcs+"."+table+"_diff(hid) ON UPDATE CASCADE")


        current_rev_sub = "(SELECT MAX(rev) FROM "+wcs+".initial_revision)"
        pcur.execute("CREATE VIEW "+wcs+"."+table+"_view AS "+
                "SELECT hid, "+cols+" "+
                "FROM (SELECT "+cols+", "+hcols+" FROM "+wcs+"."+table+"_diff "+
                            "WHERE "+branch+"_rev_end IS NULL OR "+branch+"_rev_end >= "+current_rev_sub+"+1 "
                          "UNION "+
                          "(SELECT DISTINCT ON (hid) "+cols+", t."+hcols+" FROM "+schema+"."+table+" AS t LEFT JOIN (SELECT hid FROM "+wcs+"."+table+"_diff ) AS d ON t.hid = d.hid WHERE d.hid IS NULL AND t."+branch+"_rev_begin <= "+current_rev_sub+" AND (t."+branch+"_rev_end IS NULL OR t."+branch+"_rev_end >= "+current_rev_sub+") )"+ 
                          ") AS src ")

        max_fid_sub = "( SELECT MAX(max_fid) FROM ( SELECT MAX(hid) AS max_fid FROM "+wcs+"."+table+"_diff UNION SELECT max_hid AS max_fid FROM "+wcs+".initial_revision WHERE table_name = '"+table+"') AS src )"

        pcur.execute("CREATE OR REPLACE FUNCTION myprt(error_message text) RETURNS void as $$\n"+
            "begin\n"+
                "raise notice '%', error_message;\n"+
            "end;\n"+
            "$$ language plpgsql;")

        pcur.execute("CREATE FUNCTION "+wcs+".update_"+table+"() RETURNS trigger AS $$\n"
            "BEGIN\n"
                # when we edit something we already added , we just update
                "UPDATE "+wcs+"."+table+"_diff "
                    "SET ("+cols+") "
                    "= ("+newcols+") "
                    "WHERE hid = OLD.hid AND "+branch+"_rev_begin = "+current_rev_sub+"+1 "
                    "AND (SELECT COUNT(*) FROM "+schema+"."+table+" WHERE hid = OLD.hid AND "+branch+"_rev_begin <= "+current_rev_sub+" AND ("+branch+"_rev_end IS NULL OR "+branch+"_rev_end >= "+current_rev_sub+" ) ) = 0 ;"

                "raise notice '%', OLD.hid::text||' '"
                "||(SELECT COUNT(*) FROM "+schema+"."+table+" WHERE hid = OLD.hid AND "+branch+"_rev_begin <= "+current_rev_sub+" AND ("+branch+"_rev_end IS NULL OR "+branch+"_rev_end >= "+current_rev_sub+" ) )::text;\n"

                # insert the parent in diff if not already there
                "INSERT INTO "+wcs+"."+table+"_diff "
                    "SELECT "+cols+", "+hcols+" FROM "+schema+"."+table+" "
                    "WHERE hid = OLD.hid AND "+branch+"_rev_begin <= "+current_rev_sub+" "
                    "AND (SELECT COUNT(*) FROM "+wcs+"."+table+"_diff WHERE hid = OLD.hid AND "+branch+"_rev_end = "+current_rev_sub+" ) = 0 ;"

                "raise notice '%', OLD.hid::text||' '"
                "||(SELECT COUNT(*) FROM "+wcs+"."+table+"_diff WHERE hid = OLD.hid AND "+branch+"_rev_end = "+current_rev_sub+" )::text;\n"

                # when we edit something old, we insert new
                "INSERT INTO "+wcs+"."+table+"_diff "
                    "(hid, "+cols+", "+branch+"_rev_begin, "+branch+"_parent) "
                    "SELECT "+max_fid_sub+"+1, "+newcols+", "+current_rev_sub+"+1, OLD.hid "
                    "WHERE (SELECT COUNT(*) FROM "+schema+"."+table+" WHERE hid = OLD.hid AND "+branch+"_rev_begin <= "+current_rev_sub+" AND ("+branch+"_rev_end IS NULL OR "+branch+"_rev_end >= "+current_rev_sub+" ) ) = 1; "

                "raise notice '%',  OLD.hid::text||' '"
                "||(SELECT COUNT(*) FROM "+schema+"."+table+" WHERE hid = OLD.hid AND "+branch+"_rev_begin <= "+current_rev_sub+" AND ("+branch+"_rev_end IS NULL OR "+branch+"_rev_end >= "+current_rev_sub+" ) )::text;\n"

                # update the parent in diff if it comes from the table (i.e not the diff)
                "UPDATE "+wcs+"."+table+"_diff "
                    "SET ("+branch+"_rev_end, "+branch+"_child) = ("+current_rev_sub+", "+max_fid_sub+") "
                    "WHERE hid = OLD.hid "
                    "AND (SELECT COUNT(*) FROM "+schema+"."+table+" WHERE hid = OLD.hid AND "+branch+"_rev_begin <= "+current_rev_sub+" AND ("+branch+"_rev_end IS NULL OR "+branch+"_rev_end >= "+current_rev_sub+" ) ) = 1;\n"
                "RETURN NULL;\n"
            "END;\n"
        "$$ LANGUAGE plpgsql;")

        pcur.execute("CREATE TRIGGER update_"+table+" INSTEAD OF UPDATE ON "+wcs+"."+table+"_view "
            "FOR EACH ROW EXECUTE PROCEDURE "+wcs+".update_"+table+"();")

        pcur.execute("CREATE FUNCTION "+wcs+".insert_"+table+"() RETURNS trigger AS $$\n"
            "BEGIN\n"
                "INSERT INTO "+wcs+"."+table+"_diff "+ 
                    "(hid, "+cols+", "+branch+"_rev_begin) "
                    "VALUES "
                    "("+max_fid_sub+"+1, "+newcols+", "+current_rev_sub+"+1);\n"
                "RETURN NULL;\n"
            "END;\n"
        "$$ LANGUAGE plpgsql;")

        pcur.execute("CREATE TRIGGER insert_"+table+" INSTEAD OF INSERT ON "+wcs+"."+table+"_view "
            "FOR EACH ROW EXECUTE PROCEDURE "+wcs+".insert_"+table+"();")

        pcur.execute("CREATE FUNCTION "+wcs+".delete_"+table+"() RETURNS trigger AS $$\n"
            "BEGIN\n"
                # insert if not already in diff
                "INSERT INTO "+wcs+"."+table+"_diff "
                    "SELECT "+cols+", "+hcols+" FROM epanet."+table+" "
                    "WHERE hid = OLD.hid "
                    "AND (SELECT COUNT(*) FROM "+wcs+"."+table+"_diff WHERE hid = OLD.hid) = 0;\n"
                # update if it comes from table (not diff)
                "UPDATE "+wcs+"."+table+"_diff "
                    "SET "+branch+"_rev_end = "+current_rev_sub+" "
                    "WHERE hid = OLD.hid "
                    "AND (SELECT COUNT(*) FROM  "+schema+"."+table+" WHERE hid = OLD.hid) = 1; "

                # if its just in diff, remove it from child
                "UPDATE "+wcs+"."+table+"_diff "
                    "SET "+branch+"_child = NULL "
                    "WHERE "+branch+"_child = OLD.hid "
                    "AND (SELECT COUNT(*) FROM  "+schema+"."+table+" WHERE hid = OLD.hid) = 0;\n"

                # if it's just in diff, delete it
                "DELETE FROM  "+wcs+"."+table+"_diff "
                    "WHERE hid = OLD.hid "
                    "AND (SELECT COUNT(*) FROM  "+schema+"."+table+" WHERE hid = OLD.hid) = 0;\n"
                "RETURN NULL;\n"
            "END;\n"
        "$$ LANGUAGE plpgsql;")

        pcur.execute("CREATE TRIGGER delete_"+table+" INSTEAD OF DELETE ON "+wcs+"."+table+"_view "
            "FOR EACH ROW EXECUTE PROCEDURE "+wcs+".delete_"+table+"();")

    pcur.commit()
    pcur.close()

def pg_update(pg_conn_info, working_copy_schema):
    """merge modifiactions since last update into working copy"""
    print "update"
    wcs = working_copy_schema
    if pg_unresolvedConflicts(pg_conn_info, wcs): raise RuntimeError("There are unresolved conflicts in "+wcs)

    # create the diff from previous
    # detect conflicts and create conflict layers
    # merge changes and update target_revision


    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT rev, branch, table_schema, table_name, max_hid "+
        "FROM "+wcs+".initial_revision")
    versioned_layers = pcur.fetchall()

    for [rev, branch, table_schema, table, current_max_hid] in versioned_layers:

        pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions WHERE branch = '"+branch+"'")
        [max_rev] = pcur.fetchone()
        if max_rev == rev: 
            print "Nothing new in branch "+branch+" in "+table_schema+"."+table+" since last update"
            pcur.close()
            continue

        # get the max hid 
        pcur.execute("SELECT MAX(hid) FROM "+table_schema+"."+table)
        [max_pg_hid] = pcur.fetchone()
        
        # create the diff
        pcur.execute("SELECT column_name "+
                "FROM information_schema.columns "+
                "WHERE table_schema = '"+table_schema+"' AND table_name = '"+table+"'")
        cols = ""
        for c in pcur.fetchall(): 
            if c[0] != "geom": cols += c[0]+", "
        cols = cols[:-2] # remove last coma and space

        pcur.execute("SELECT srid, type "+
            "FROM geometry_columns "+
            "WHERE f_table_schema = '"+table_schema+"' AND f_table_name ='"+table+"' AND f_geometry_column = 'geom'")
        [srid, geom_type] = pcur.fetchone()
        pcur.execute( "DROP TABLE IF EXISTS "+wcs+"."+table+"_update_diff")
        pcur.execute( "CREATE TABLE "+wcs+"."+table+"_update_diff AS "+
                "SELECT "+cols+", geom::geometry('"+geom_type+"', "+str(srid)+") AS geom "+
                "FROM "+table_schema+"."+table+" "+
                "WHERE "+branch+"_rev_end = "+str(rev)+" OR "+branch+"_rev_begin > "+str(rev))
        pcur.execute( "ALTER TABLE "+wcs+"."+table+"_update_diff "+
                "ADD CONSTRAINT "+table+"_"+branch+"_hid_pk PRIMARY KEY (hid)") 

        # update the initial revision 
        pcur.execute("UPDATE "+wcs+".initial_revision SET rev = "+str(max_rev)+", max_hid = "+str(max_pg_hid)+" WHERE table_name = '"+table+"'")
        
        pcur.execute("UPDATE "+wcs+"."+table+"_diff "+
                "SET "+branch+"_rev_end = "+str(max_rev)+" "+
                "WHERE "+branch+"_rev_end = "+str(rev))
        pcur.execute("UPDATE "+wcs+"."+table+"_diff "+
                "SET "+branch+"_rev_begin = "+str(max_rev+1)+" "+
                "WHERE "+branch+"_rev_begin = "+str(rev+1))

        bump = max_pg_hid - current_max_hid
        assert( bump >= 0) 
        # now bump the hids of inserted rows in working copy
        # parents will be updated thanks to the ON UPDATE CASCADE
        pcur.execute("UPDATE "+wcs+"."+table+"_diff "+
                "SET hid = hid + "+str(bump)+" "
                "WHERE "+branch+"_rev_begin = "+str(max_rev+1))

        # detect conflicts: conflict occur if two lines with the same hid have
        # been modified (i.e. have a non null child) or one has been removed
        # and the other modified
        pcur.execute("DROP VIEW IF EXISTS "+wcs+"."+table+"_conflicts_hid")
        pcur.execute("CREATE VIEW "+wcs+"."+table+"_conflicts_hid AS "+
            "SELECT DISTINCT d.hid as conflict_deleted_hid "+
            "FROM "+wcs+"."+table+"_diff AS d, "+wcs+"."+table+"_update_diff AS ud "+
            "WHERE d.hid = ud.hid "+
                "AND (d."+branch+"_child != ud."+branch+"_child "+
                "OR (d."+branch+"_child IS NULL AND ud."+branch+"_child IS NOT NULL) "
                "OR (d."+branch+"_child IS NOT NULL AND ud."+branch+"_child IS NULL)) ")
        pcur.execute("SELECT conflict_deleted_hid FROM  "+wcs+"."+table+"_conflicts_hid" )
        if pcur.fetchone():
            print "there are conflicts"
            # add layer for conflicts
            pcur.execute("DROP TABLE IF EXISTS "+wcs+"."+table+"_cflt ")
            pcur.execute("CREATE TABLE "+wcs+"."+table+"_cflt AS "+
                # insert new features from mine
                "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, 'modified' AS action, "+cols+" "+
                "FROM "+wcs+"."+table+"_diff, "+wcs+"."+table+"_conflicts_hid AS cflt "+
                "WHERE hid = (SELECT "+branch+"_child FROM "+wcs+"."+table+"_diff "+
                                     "WHERE hid = conflict_deleted_hid) "+
                "UNION ALL "
                # insert new features from theirs
                "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, 'modified' AS action, "+cols+" "+
                "FROM "+wcs+"."+table+"_update_diff "+", "+wcs+"."+table+"_conflicts_hid AS cflt "+
                "WHERE hid = (SELECT "+branch+"_child FROM "+wcs+"."+table+"_update_diff "+
                                     "WHERE hid = conflict_deleted_hid) "+
                 # insert deleted features from mine
                "UNION ALL "+
                "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, 'deleted' AS action, "+cols+" "+
                "FROM "+wcs+"."+table+"_diff, "+wcs+"."+table+"_conflicts_hid AS cflt "+
                "WHERE hid = conflict_deleted_hid AND "+branch+"_child IS NULL "+
                 # insert deleted features from theirs
                "UNION ALL "+
                "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, 'deleted' AS action, "+cols+" "+
                "FROM "+wcs+"."+table+"_update_diff, "+wcs+"."+table+"_conflicts_hid AS cflt "+
                "WHERE hid = conflict_deleted_hid AND "+branch+"_child IS NULL" )

            # identify conflicts for deleted 
            pcur.execute("UPDATE "+wcs+"."+table+"_cflt "+ "SET conflict_id = hid "+ "WHERE action = 'deleted'")

            # now follow child if any for 'theirs' 'modified' since several edition could be made
            # we want the very last child
            while True:
                pcur.execute("SELECT conflict_id, hid, "+branch+"_child FROM "+wcs+"."+table+"_cflt WHERE origin='theirs' AND action='modified' AND "+branch+"_child IS NOT NULL");
                r = pcur.fetchall()
                if not r : break
                # replaces each entries by it's child
                for [cflt_id, fid, child] in r:
                    pcur.execute("DELETE FROM "+wcs+"."+table+"_cflt WHERE hid = "+str(fid))
                    pcur.execute("INSERT INTO "+wcs+"."+table+"_cflt "+
                        "SELECT "+str(cflt_id)+" AS conflict_id, 'theirs' AS origin, 'modified' AS action, "+cols+" FROM "+wcs+"."+table+"_update_diff "+
                        "WHERE hid = "+str(child)+" AND "+branch+"_rev_end IS NULL" );
                    pcur.execute("INSERT INTO "+wcs+"."+table+"_cflt "+
                        "SELECT "+str(cflt_id)+" AS conflict_id, 'theirs' AS origin, 'deleted' AS action, "+cols+" FROM "+wcs+"."+table+"_update_diff "+
                        "WHERE hid = "+str(child)+" AND "+branch+"_rev_end IS NOT NULL" );

            # create trigers such that on delete the conflict is resolved
            # if we delete 'theirs', we set their child to our fid and their rev_end
            # if we delete 'mine'... well, we delete 'mine'

            pcur.execute("SELECT column_name "+
                    "FROM information_schema.columns "+
                    "WHERE table_schema = '"+wcs+"' AND table_name = '"+table+"_diff'")
            cols = ""
            for c in pcur.fetchall(): 
                cols += c[0]+", "
            cols = cols[:-2] # remove last coma and space

            pcur.execute("CREATE OR REPLACE VIEW "+wcs+"."+table+"_conflicts AS SELECT * FROM  "+wcs+"."+table+"_cflt" )

            pcur.execute("CREATE OR REPLACE RULE delete_"+table+"_conflicts AS ON DELETE TO "+wcs+"."+table+"_conflicts\n"+
                "DO INSTEAD ("+
                    "DELETE FROM "+wcs+"."+table+"_diff "+
                    "WHERE hid = OLD.hid AND OLD.origin = 'mine';\n"+

                    # we need to insert their parent to update it
                    "INSERT INTO "+wcs+"."+table+"_diff("+cols+") "+
                    "SELECT "+cols+" FROM "+table_schema+"."+table+" "+
                    "WHERE hid = OLD."+branch+"_parent AND OLD.origin = 'theirs';\n"+

                    "UPDATE "+wcs+"."+table+"_diff "+
                    "SET "+branch+"_child = (SELECT hid FROM "+wcs+"."+table+"_cflt WHERE origin = 'mine' AND conflict_id = OLD.conflict_id), "+branch+"_rev_end = "+str(max_rev)+" "
                    "WHERE hid = OLD.hid AND OLD.origin = 'theirs';\n"+

                    "UPDATE "+wcs+"."+table+"_diff "+
                    "SET "+branch+"_parent = OLD.hid "+
                    "WHERE hid = (SELECT hid FROM "+wcs+"."+table+"_cflt WHERE origin = 'mine' AND conflict_id = OLD.conflict_id) AND OLD.origin = 'theirs';\n"+

                    "DELETE FROM "+wcs+"."+table+"_cflt "+
                    "WHERE conflict_id = OLD.conflict_id;\n"+
                ")")

            pcur.commit()

            pcur.execute("ALTER TABLE "+wcs+"."+table+"_cflt "+
                "ADD CONSTRAINT "+table+"_"+branch+"conflicts_hid_pk PRIMARY KEY (hid)")

    pcur.commit()
    pcur.close()

def pg_commit(pg_conn_info, working_copy_schema, commit_msg):
    """merge modifications into database
    returns the number of updated layers"""
    wcs = working_copy_schema

    unresolved = pg_unresolvedConflicts(pg_conn_info, wcs)
    if unresolved: raise RuntimeError("There are unresolved conflicts in "+wcs+" for table(s) "+', '.join(unresolved) )

    lateBy = pg_late(pg_conn_info, wcs)
    if lateBy:  raise RuntimeError("The table '"+table+"' in file '"+sqlite_filename+"' is not up to date. It's late by "+str(lateBy)+" commit(s).\n\nPlease update before commiting your modifications")

    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT rev, branch, table_schema, table_name "+
        "FROM "+wcs+".initial_revision")
    versioned_layers = pcur.fetchall()

    if not versioned_layers: raise RuntimeError("Cannot find a versioned layer in "+wcs)


    nb_of_updated_layer = 0
    next_rev = 0;
    for [rev, branch, table_schema, table] in versioned_layers:
        if next_rev: assert( next_rev == rev + 1 )
        else: next_rev = rev + 1

        history_columns = ['hid'] 
        pcur.execute("SELECT DISTINCT branch FROM "+table_schema+".revisions")
        for [b] in pcur.fetchall():
            history_columns.extend([b+'_rev_end', b+'_rev_begin', b+'_child', b+'_parent'])
        pcur.execute("SELECT column_name "+
                "FROM information_schema.columns "+
                "WHERE table_schema = '"+table_schema+"' AND table_name = '"+table+"'")
        cols = ""
        for [c] in pcur.fetchall(): 
            if c not in history_columns:
                cols = c+", "+cols
        cols = cols[:-2] # remove last coma and space
        hcols = "hid, "+branch+"_rev_begin, "+branch+"_rev_end, "+branch+"_parent, "+branch+"_child"

        pcur.execute( "SELECT hid FROM "+wcs+"."+table+"_diff")
        there_is_something_to_commit = pcur.fetchone()

        if not there_is_something_to_commit: 
            print "nothing to commit for ", table
            continue
        nb_of_updated_layer += 1

        pcur.execute("SELECT rev FROM "+table_schema+".revisions WHERE rev = "+str(rev+1))
        if not pcur.fetchone():
            print "inserting rev ", str(rev+1)
            pcur.execute("INSERT INTO "+table_schema+".revisions (rev, commit_msg, branch, author) VALUES ("+str(rev+1)+", '"+commit_msg+"', '"+branch+"', '"+get_username()+"')")

        # insert inserted and modified
        pcur.execute("INSERT INTO "+table_schema+"."+table+" ("+cols+", "+hcols+") "+
            "SELECT "+cols+", "+hcols+" FROM "+wcs+"."+table+"_diff "+
            "WHERE "+branch+"_rev_begin = "+str(rev+1))

        # update deleted and modified 
        pcur.execute("UPDATE "+table_schema+"."+table+" AS dest "+
                "SET ("+branch+"_rev_end, "+branch+"_child)=(src."+branch+"_rev_end, src."+branch+"_child) "+
                "FROM "+wcs+"."+table+"_diff AS src "+
                "WHERE dest.hid = src.hid AND src."+branch+"_rev_end = "+str(rev))

        # clears the diff
        pcur.execute("DELETE FROM "+wcs+"."+table+"_diff")

    pcur.commit()
    pcur.close()

def pg_unresolvedConflicts(pg_conn_info, working_copy_schema):
    """return a list of tables with unresolved conflicts"""
    found = []
    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='"+working_copy_schema+"' AND table_name LIKE '%_cflt'")
    for table_conflicts in pcur.fetchall():
        print 'table_conflicts:',table_conflicts[0]
        pcur.execute("SELECT * FROM "+working_copy_schema+"."+table_conflicts[0])
        if pcur.fetchone():
            found.append( table_conflicts[0][:-5] )
    pcur.commit()
    pcur.close()
    return found

def pg_late(pg_conn_info, working_copy_schema):
    """Return 0 if up to date, the number of commits in between otherwize"""
    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("SELECT rev, branch, table_schema, table_name "+
        "FROM "+working_copy_schema+".initial_revision")
    versioned_layers = pcur.fetchall()
    if not versioned_layers: raise RuntimeError("Cannot find versioned layer in "+working_copy_schema)

    lateBy = 0;

    for [rev, branch, table_schema, table] in versioned_layers:
        pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions WHERE branch = '"+branch+"'")
        [max_rev] = pcur.fetchone()
        lateBy = max(max_rev - rev, lateBy)

    return lateBy

