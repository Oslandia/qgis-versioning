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

   def execute(self, sql):
       if not self.begun:
           self.begun = True
           print self.db_type, 'BEGIN;'
           if self.log : self.log.write( 'BEGIN;\n')
       print self.db_type, sql, ';'
       if self.log : self.log.write(sql+';\n')
       self.cur.execute( sql )

   def fetchall(self):
       return self.cur.fetchall()

   def fetchone(self):
       return self.cur.fetchone()

   def commit(self):
       print self.db_type, 'END;'
       if self.log : self.log.write('END;\n')
       self.begun = False;
       self.con.commit()

   def close(self):
       if self.begun : 
           print self.db_type, 'END;'
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
            if not scur.fetchone(): # no record, insert
                scur.execute("INSERT INTO initial_revision(rev, branch, table_schema, table_name, conn_info, max_hid) VALUES ("+str(current_rev)+", '"+branch+"', '"+schema+"', '"+table+"', '"+escapeQuotes(pg_conn_info)+"', "+str(max_pg_hid)+")" )
            scur.commit()
            scur.close()

        scur = Db(dbapi2.connect(sqlite_filename))

        # create views and triggers in spatilite db
        scur.execute("PRAGMA table_info("+table+")")
        cols = ""
        newcols = ""
        hcols = {}
        for r in scur.fetchall():
            if   r[1][-10:] == "_rev_begin" : pass
            elif r[1][-8:]  == "_rev_end" : pass
            elif r[1][-6:]  == "_child" : pass
            elif r[1][-7:]  == "_parent" : pass
            elif r[1]  == "OGC_FID" : pass
            else : 
                cols += r[1] + ", "
                newcols += "new."+r[1]+", "
        cols = cols[:-2]
        newcols = newcols[:-2] # remove last coma
        print cols
        print hcols
        sql = "CREATE VIEW "+table+"_view "+"AS SELECT ROWID AS ROWID, OGC_FID, "+cols+" FROM "+table+" WHERE "+branch+"_rev_end IS NULL"
        print sql
        scur.execute(sql)

        max_fid_sub = "( SELECT MAX(max_fid) FROM ( SELECT MAX(OGC_FID) AS max_fid FROM "+table+" UNION SELECT max_hid AS max_fid FROM initial_revision WHERE table_name = '"+table+"') )"

        scur.execute("DELETE FROM views_geometry_columns WHERE f_table_name = '"+table+"_conflicts'")
        sql = "INSERT INTO views_geometry_columns "+"(view_name, view_geometry, view_rowid, f_table_name, f_geometry_column) "+"VALUES"+"('"+table+"_view', 'GEOMETRY', 'ROWID', '"+table+"', 'GEOMETRY')"
        print sql 
        scur.execute(sql)  
         
        sql =("CREATE TRIGGER update_"+table+" INSTEAD OF UPDATE ON "+table+"_view\n"+
              "BEGIN\n"+
                "INSERT INTO "+table+" "
                "(OGC_FID, "+cols+", "+branch+"_rev_begin, "+branch+"_parent) "+
                "VALUES "
                "("+max_fid_sub+"+1, "+newcols+", (SELECT rev+1 FROM initial_revision WHERE table_name = '"+table+"'), old.OGC_FID);\n"+
                "UPDATE "+table+" SET "+branch+"_rev_end = (SELECT rev FROM initial_revision WHERE table_name = '"+table+"'), "+branch+"_child = "+max_fid_sub+" WHERE OGC_FID = old.OGC_FID;\n"+
              "END")
        print sql
        scur.execute(sql)  

        sql =("CREATE TRIGGER insert_"+table+" INSTEAD OF INSERT ON "+table+"_view\n"+
              "BEGIN\n"+
                "INSERT INTO "+table+" "+ 
                "(OGC_FID, "+cols+", "+branch+"_rev_begin) "+
                "VALUES "+
                "("+max_fid_sub+"+1, "+newcols+", (SELECT rev+1 FROM initial_revision WHERE table_name = '"+table+"'));\n"+
              "END")
        print sql
        scur.execute(sql)  

        sql =("CREATE TRIGGER delete_"+table+" INSTEAD OF DELETE ON "+table+"_view\n"+
              "BEGIN\n"+
                "UPDATE "+table+" SET "+branch+"_rev_end = (SELECT rev FROM initial_revision WHERE table_name = '"+table+"') WHERE OGC_FID = old.OGC_FID;\n"+
              "END")
        print sql 
        scur.execute(sql)
        
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
        conflict_for_this_layer = False

        print conn_info
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
        print 'max_pg_hid ',max_pg_hid

        # create the diff
        diff_schema = table_schema+"_"+branch+"_"+str(rev)+"_to_"+str(max_rev)+"_diff"
        pcur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"+diff_schema+"'")
        if not pcur.fetchone(): pcur.execute("CREATE SCHEMA "+diff_schema)

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
                "ADD CONSTRAINT "+table+"_"+branch+"__hid_pk PRIMARY KEY (hid)") 
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
        # note that to do that, we need to set a negative value because the UPDATE is
        # not implemented correctly according to:
        # http://stackoverflow.com/questions/19381350/simulate-order-by-in-sqlite-update-to-handle-uniqueness-constraint
        scur.execute("UPDATE "+table+" "+
                "SET OGC_FID = -(OGC_FID + "+str(bump)+") "+
                "WHERE "+branch+"_rev_begin = "+str(max_rev+1))
        scur.execute("UPDATE "+table+" SET OGC_FID = -OGC_FID WHERE OGC_FID < 0")
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
        scur.execute("SELECT * FROM  "+table+"_conflicts_ogc_fid" )
        if scur.fetchone():
            print "there are conflicts"
            # add layer for conflicts
            scur.execute("DROP TABLE IF EXISTS "+table+"_conflicts ")
            sql=("CREATE TABLE "+table+"_conflicts AS "+
                # insert new features from mine
                "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, 'modified' AS action, "+cols+" FROM "+table+", "+table+"_conflicts_ogc_fid AS cflt "+
                "WHERE OGC_FID = (SELECT "+branch+"_child FROM "+table+" "+
                                     "WHERE OGC_FID = conflict_deleted_fid) "+
                "UNION ALL "
                # insert new features from theirs
                "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, 'modified' AS action, "+cols+" FROM "+table+"_diff "+", "+table+"_conflicts_ogc_fid AS cflt "+
                "WHERE OGC_FID = (SELECT "+branch+"_child FROM "+table+"_diff "+
                                     "WHERE OGC_FID = conflict_deleted_fid) "+
                 # insert deleted features from mine
                "UNION ALL "+
                "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, 'deleted' AS action, "+cols+" FROM "+table+", "+table+"_conflicts_ogc_fid AS cflt "+
                "WHERE OGC_FID = conflict_deleted_fid AND "+branch+"_child IS NULL "+
                 # insert deleted features from theirs
                "UNION ALL "+
                "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, 'deleted' AS action, "+cols+" FROM "+table+"_diff, "+table+"_conflicts_ogc_fid AS cflt "+
                "WHERE OGC_FID = conflict_deleted_fid AND "+branch+"_child IS NULL" )
            print sql
            scur.execute(sql)

            # identify conflicts for deleted 
            scur.execute("UPDATE "+table+"_conflicts "+
                    "SET conflict_id = OGC_FID "+
                    "WHERE action = 'deleted'")
            scur.execute("DELETE FROM geometry_columns WHERE f_table_name = '"+table+"_conflicts'")
            scur.execute("SELECT RecoverGeometryColumn('"+table+"_conflicts', 'GEOMETRY', (SELECT srid FROM geometry_columns WHERE f_table_name='"+table+"'), (SELECT type FROM geometry_columns WHERE f_table_name='"+table+"'), 'XY')")
            
            scur.execute("CREATE UNIQUE INDEX IF NOT EXISTS "+table+"_conflicts_idx ON "+table+"_conflicts(OGC_FID)")

            # create trigers such that on delete the conflict is resolved
            # for modified on both side:
            #   - if we delete 'mine' 
            #           - we delete the inserted on our side, 
            #           - copy their inserted 
            #           - and change our parent's child (replace by theirs)
            #   - if we delete 'theirs' 
            #           - we change the parent on our side to match theirs
            #           - we then insert their child
            #           - and set the end_revision and child of this record to point to our child
            # for deleted on our side and modified on theirs:
            #   - if we delete 'mine' 
            #           - we just replace ours parent's child (delete) by theirs (modified) 
            #           - and insert their child
            #   - if we delete 'theirs' 
            #           - we do the same thing, 
            #           - plus we set the end revision on their child
            # for modified on our side and deleted on theirs:
            #   - if we delete 'mine' 
            #           - we remove our child 
            #           - and set the parent as theirs (deleted)
            #   - if we delete 'theirs' 
            #           - we set our parent as theirs (deleted without children)
            #           - we update our child to set the parent to null
            # in all case we end by deleting the 2 conflict rows in the conflict table
            # and remove their parent and child from the diff

            modified_on_both_sides_delete_mine = ("old.action = 'modified' AND old.origin = 'mine' "+
                "AND (SELECT action FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id ) = 'modified'")

            modified_on_both_sides_delete_theirs = ("old.action = 'modified' AND old.origin = 'theirs' "+
                "AND (SELECT action FROM "+table+"_conflicts WHERE origin = 'mine' AND conflict_id = old.conflict_id ) = 'modified'")

            deleted_on_our_sides_modified_on_theirs_delete_mine = ("old.action = 'deleted' AND old.origin = 'mine' "+
                "AND (SELECT action FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id ) = 'modified'")

            deleted_on_our_sides_modified_on_theirs_delete_theirs = ("old.action = 'modified' AND old.origin = 'theirs' "+
                "AND (SELECT action FROM "+table+"_conflicts WHERE origin = 'mine' AND conflict_id = old.conflict_id ) = 'deleted'")

            modified_on_our_sides_deleted_on_theirs_delete_mine = ("old.action = 'modified' AND old.origin = 'mine' "+
                "AND (SELECT action FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id ) = 'deleted'")

            modified_on_our_sides_deleted_on_theirs_delete_theirs = ("old.action = 'deleted' AND old.origin = 'theirs' "+
                "AND (SELECT action FROM "+table+"_conflicts WHERE origin = 'mine' AND conflict_id = old.conflict_id ) = 'modified'")

            scur.execute("DROP TRIGGER IF EXISTS delete_"+table+"_conflicts")
            sql =("CREATE TRIGGER delete_"+table+"_conflicts AFTER DELETE ON "+table+"_conflicts\n"+
                "BEGIN\n"+
                    "DELETE FROM "+table+" "+
                    "WHERE OGC_FID = old.OGC_FID AND "+modified_on_both_sides_delete_mine+";\n"+

                    "INSERT INTO "+table+"("+cols+") "+
                    "SELECT "+cols+" FROM "+table+"_diff "+
                    "WHERE OGC_FID = (SELECT OGC_FID FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id ) "+
                        "AND "+modified_on_both_sides_delete_mine+";\n"+

                    "REPLACE INTO "+table+"("+cols+") "+
                    "SELECT "+cols+" FROM "+table+"_diff "+
                    "WHERE OGC_FID = old."+branch+"_parent AND "+modified_on_both_sides_delete_mine+";\n"+

                    "UPDATE "+table+" "+
                    "SET "+branch+"_child = (SELECT OGC_FID FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id) "+
                    "WHERE OGC_FID = old."+branch+"_parent "+
                        "AND "+modified_on_both_sides_delete_theirs+";\n"+

                    "INSERT INTO "+table+"("+cols+") "+
                    "SELECT "+cols+" FROM "+table+"_diff "+
                    "WHERE OGC_FID = old.OGC_FID "+
                        "AND "+modified_on_both_sides_delete_theirs+";\n"+

                    "UPDATE "+table+" "+
                    "SET "+branch+"_child = (SELECT OGC_FID FROM "+table+"_conflicts WHERE origin = 'mine' AND conflict_id = old.conflict_id), "+
                           branch+"_rev_end = "+str(max_rev)+" "+
                    "WHERE OGC_FID = old.OGC_FID "+
                        "AND "+modified_on_both_sides_delete_theirs+";\n"+

                    "REPLACE INTO "+table+"("+cols+") "+
                    "SELECT "+cols+" FROM "+table+"_diff "+
                    "WHERE OGC_FID = old."+branch+"_parent "+
                        "AND "+deleted_on_our_sides_modified_on_theirs_delete_mine+";\n"+

                    "INSERT INTO "+table+"("+cols+") "+
                    "SELECT "+cols+" FROM "+table+"_diff "+
                    "WHERE OGC_FID = (SELECT OGC_FID FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id) "+
                        "AND "+deleted_on_our_sides_modified_on_theirs_delete_mine+";\n"+

                    "REPLACE INTO "+table+"("+cols+") "+
                    "SELECT "+cols+" FROM "+table+"_diff "+
                    "WHERE OGC_FID = old."+branch+"_parent "+
                        "AND "+deleted_on_our_sides_modified_on_theirs_delete_theirs+";\n"+

                    "INSERT INTO "+table+"("+cols+") "+
                    "SELECT "+cols+" FROM "+table+"_diff "+
                    "WHERE OGC_FID = old.OGC_FID "+
                        "AND "+deleted_on_our_sides_modified_on_theirs_delete_theirs+";\n"+

                    "UPDATE "+table+" "+
                    "SET  "+branch+"_rev_end = "+str(max_rev)+" "+
                    "WHERE OGC_FID = old.OGC_FID "+
                        "AND "+deleted_on_our_sides_modified_on_theirs_delete_theirs+";\n"+

                    "DELETE FROM "+table+" "+
                    " WHERE OGC_FID = old.OGC_FID "+
                        "AND "+modified_on_our_sides_deleted_on_theirs_delete_mine+";\n"+

                    "REPLACE INTO "+table+"("+cols+") "+
                    "SELECT "+cols+" FROM "+table+"_diff "+
                    "WHERE OGC_FID = old."+branch+"_parent "+
                        "AND "+modified_on_our_sides_deleted_on_theirs_delete_mine+";\n"+

                    "REPLACE INTO "+table+"("+cols+") "+
                    "SELECT "+cols+" FROM "+table+"_diff "+
                    "WHERE OGC_FID = old."+branch+"_parent "+
                        "AND "+deleted_on_our_sides_modified_on_theirs_delete_theirs+";\n"+

                    "UPDATE "+table+" "
                    "SET "+branch+"_parent = NULL "+
                    "WHERE OGC_FID = old.OGC_FID "+
                        "AND "+deleted_on_our_sides_modified_on_theirs_delete_theirs+";\n"+

                    "DELETE FROM "+table+"_conflicts "+
                    "WHERE conflict_id = old.conflict_id;\n"+

                    "DELETE FROM "+table+"_diff "+
                    "WHERE (OGC_FID = (SELECT OGC_FID FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id ) "+
                            "OR OGC_FID = (SELECT "+branch+"_parent FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id ));\n"+
                "END")
            print sql
            scur.execute(sql)

            scur.commit()

            conflict_for_this_layer = True

        scur.execute("CREATE UNIQUE INDEX IF NOT EXISTS "+table+"_diff_idx ON "+table+"_diff(OGC_FID)")
        if conflict_for_this_layer: 
            # insert inserted and modified and update deleted and modified that have no conflicts
            scur.execute("INSERT OR REPLACE INTO "+table+" ("+cols+") "+
                "SELECT "+cols+" "+
                "FROM "+table+"_diff "+
                    "LEFT JOIN (SELECT OGC_FID AS conflict_id FROM "+table+"_conflicts WHERE origin = 'theirs' "+
                         "UNION SELECT trunk_parent AS conflict_id FROM "+table+"_conflicts WHERE origin = 'theirs') AS c "+
                    "ON OGC_FID = c.conflict_id "+
                    "WHERE c.conflict_id IS NULL")
        else:
            # insert and replace all
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
    scur = Db(dbapi2.connect(sqlite_filename))
    scur.execute("SELECT rev "+ "FROM initial_revision")
    rev = scur.fetchall()
    revision = 0
    for [r] in rev:
        if revision : assert( r == revision )
        else : revision = r
    return revision

def commit(sqlite_filename, commit_msg):
    """merge modifiactions into database
    returns the number of updated layers"""
    if unresolvedConflicts(sqlite_filename): raise RuntimeError("There are unresolved conflicts in "+sqlite_filename)

    lateBy = late(sqlite_filename)
    if lateBy:  raise RuntimeError("The table '"+table+"' in file '"+sqlite_filename+"' is not up to date. It's late by "+str(lateBy)+" commit(s).\n\nPlease update before commiting your modifications")

    scur = Db(dbapi2.connect(sqlite_filename))
    scur.execute("SELECT rev, branch, table_schema, conn_info, table_name "+
        "FROM initial_revision")
    versioned_layers = scur.fetchall()

    if not versioned_layers: raise RuntimeError("Cannot find versioned layer "+table+" in "+sqlite_filename)

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

        pcur.commit()
        # update deleted and modified 
        pcur.execute("UPDATE "+table_schema+"."+table+" AS dest "+
                "SET ("+branch+"_rev_end, "+branch+"_child)=(src."+branch+"_rev_end, src."+branch+"_child) "+
                "FROM "+diff_schema+"."+table+"_diff AS src "+
                "WHERE dest.hid = src.hid AND src."""+branch+"_rev_end = "+str(rev))
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

