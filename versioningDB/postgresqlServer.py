# -*- coding: utf-8 -*-

from __future__ import absolute_import
from .utils import *
import psycopg2

from itertools import zip_longest

DEBUG=False

class pgVersioningServer(object):    
    
    def revision(self, connection ):
        (pg_conn_info, working_copy_schema) = connection
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
    
    def late(self, connection ):
        (pg_conn_info, working_copy_schema) = connection
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

    def update(self, connection ):
        (pg_conn_info, working_copy_schema) = connection
        """merge modifications since last update into working copy"""
        if DEBUG: print("update")
        wcs = working_copy_schema
        if self.unresolved_conflicts([pg_conn_info, wcs]):
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
                if DEBUG: print("Nothing new in branch "+branch+" "
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
                if DEBUG: print("there are conflicts")
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
        
    # functions checkout, update and commit for a posgres working copy
    # we don't want to duplicate data
    # we need the initial_revision table all the same
    # for each table we need a diff and a view and triggers
    
    def checkout(self, connection, pg_table_names, selected_feature_lists = []):
        (pg_conn_info, working_copy_schema) = connection
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
        for pg_table_name, feature_list in list(zip_longest(pg_table_names, selected_feature_lists)):
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
    
            if feature_list:
                additional_filter = "AND t.{pkey} IN ({features})".format(
                        pkey=pkey,
                        features = ','.join(str(f) for f in feature_list)
                        )
            else:
                additional_filter = ""
    
            current_rev_sub = "(SELECT MAX(rev) FROM "+wcs+".initial_revision)"
            pcur.execute("""
                CREATE VIEW {wcs}.{table}_view AS 
                    SELECT {pkey}, {cols}
                    FROM {wcs}.{table}_diff 
                    WHERE ({branch}_rev_end IS NULL OR {branch}_rev_end >= {current_rev_sub}+1 ) 
                    AND {branch}_rev_begin IS NOT NULL 
                    UNION ALL
                    SELECT /*DISTINCT ON ({pkey})*/ t.{pkey}, {cols}
                    FROM {schema}.{table} AS t 
                    LEFT JOIN (SELECT {pkey} FROM {wcs}.{table}_diff) AS d ON t.{pkey} = d.{pkey} 
                    WHERE d.{pkey} IS NULL 
                    AND t.{branch}_rev_begin <= {current_rev_sub} 
                    AND ((t.{branch}_rev_end IS NULL 
                        OR t.{branch}_rev_end >= {current_rev_sub}) 
                        AND t.{branch}_rev_begin IS NOT NULL)
                    {additional_filter}
                """.format(
                    wcs=wcs,
                    schema=schema,
                    table=table,
                    pkey=pkey,
                    cols=cols,
                    hcols=hcols,
                    branch=branch,
                    current_rev_sub=current_rev_sub,
                    additional_filter=additional_filter
                ))
    
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
                    "RETURN NEW;\n"
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
                    "RETURN NEW;\n"
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
                    "RETURN OLD;\n"
                "END;\n"
            "$$ LANGUAGE plpgsql;")
    
            pcur.execute("CREATE TRIGGER delete_"+table+" "
                "INSTEAD OF DELETE ON "+wcs+"."+table+"_view "
                "FOR EACH ROW EXECUTE PROCEDURE "+wcs+".delete_"+table+"();")
    
        pcur.commit()
        pcur.close()
    
    def unresolved_conflicts(self, connection ):
        (pg_conn_info, working_copy_schema) = connection
        """return a list of tables with unresolved conflicts"""
        found = []
        pcur = Db(psycopg2.connect(pg_conn_info))
        pcur.execute("SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='"+working_copy_schema+"' "
            "AND table_name LIKE '%_cflt'")
        for table_conflicts in pcur.fetchall():
            if DEBUG: print('table_conflicts:', table_conflicts[0])
            pcur.execute("SELECT * "
                "FROM "+working_copy_schema+"."+table_conflicts[0])
            if pcur.fetchone():
                found.append( table_conflicts[0][:-5] )
        pcur.commit()
        pcur.close()
        return found
    
    def commit(self, connection, commit_msg, commit_user=''):
        (pg_conn_info, working_copy_schema) = connection
        """merge modifications into database
        returns the number of updated layers"""
        wcs = working_copy_schema
    
        unresolved = self.unresolved_conflicts([pg_conn_info, wcs])
        if unresolved:
            raise RuntimeError("There are unresolved conflicts in "+wcs+" "
                "for table(s) "+', '.join(unresolved) )
    
        late_by = self.late([pg_conn_info, wcs])
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
                if DEBUG: print("nothing to commit for ", table)
                continue
            nb_of_updated_layer += 1
    
            pcur.execute("SELECT rev FROM "+table_schema+".revisions "
                "WHERE rev = "+str(rev+1))
            if not pcur.fetchone():
                if DEBUG: print("inserting rev ", str(rev+1))
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
    
            if DEBUG: print("truncate diff for ", table)
            # clears the diff
            pcur.execute("TRUNCATE TABLE "+wcs+"."+table+"_diff CASCADE")
            #pcur.execute("DELETE FROM "+wcs+"."+table+"_diff_pkey")
            if DEBUG: print("diff truncated for ", table)
    
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
    
