# -*- coding: utf-8 -*-

from __future__ import absolute_import
from .utils import *
from itertools import zip_longest
from collections import OrderedDict

import os
DEBUG=False

class spVersioning(object):
    
    def revision(self, connection ):
        sqlite_filename = connection[0]
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
    
    def late(self, connection ):
        (sqlite_filename, pg_conn_info) = connection
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
    
    def update(self, connection ):
        (sqlite_filename, pg_conn_info) = connection
        """merge modifications since last update into working copy"""
        if DEBUG: print("update")
        if self.unresolved_conflicts([sqlite_filename]):
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
                if DEBUG: print("Nothing new in branch "+branch+" in "+table_schema+"."
                    +table+" since last update")
                pcur.close()
                continue
    
            # get the max pkey
            pkey = pg_pk( pcur, table_schema, table )
            pgeom = pg_geom( pcur, table_schema, table )
            pgeoms = pg_geoms(pcur, table_schema, table)
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
                if col[0] not in pgeoms and col[0] not in other_branches_columns:
                    cols += quote_ident(col[0])+", "
            cols = cols[:-2] # remove last coma and space
            pcur.execute("""
                SELECT f_geometry_column, srid, type
                FROM geometry_columns
                WHERE f_table_schema = '{table_schema}'
                AND f_table_name = '{table}'
                """.format(table_schema=table_schema, table=table))
            geoms_name_srid_type = pcur.fetchall()
            geom = ""
            for geom_name, srid, geom_type in geoms_name_srid_type:
                geom += ", {geom_name}::geometry('{geom_type}', {srid})".format(
                        geom_name=geom_name, geom_type=geom_type, srid=srid) 
    
            pcur.execute( "DROP TABLE IF EXISTS "+diff_schema+"."+table+"_diff")
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
                   '-lco', 'FID=ogc_fid',
                   '-lco', 'GEOMETRY_NAME={}'.format(pgeom),
                   '-f', 'SQLite',
                   '-update',
                   '"' + sqlite_filename + '"',
                   'PG:"'+pg_conn_info+'"',
                   diff_schema+'.'+table+"_diff",
                   '-nln', table+"_diff"]
            if DEBUG: print(' '.join(cmd))
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
                    "SET ogc_fid = -ogc_fid  "
                    "WHERE "+branch+"_rev_begin = "+str(max_rev+1))
            scur.execute("UPDATE "+table+" "
                "SET ogc_fid = "+str(bump)+"-ogc_fid WHERE ogc_fid < 0")
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
                "SELECT DISTINCT sl.ogc_fid as conflict_deleted_fid "
                "FROM "+table+" AS sl, "+table+"_diff AS pg "
                "WHERE sl.ogc_fid = pg.ogc_fid "
                    "AND sl."+branch+"_child != pg."+branch+"_child")
            scur.execute("SELECT conflict_deleted_fid "
                "FROM  "+table+"_conflicts_ogc_fid" )
            if scur.fetchone():
                if DEBUG: print("there are conflicts")
                # add layer for conflicts
                scur.execute("DROP TABLE IF EXISTS "+table+"_conflicts ")
                scur.execute("CREATE TABLE "+table+"_conflicts AS "
                    # insert new features from mine
                    "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, "
                    "'modified' AS action, "+cols+" "
                    "FROM "+table+", "+table+"_conflicts_ogc_fid AS cflt "
                    "WHERE ogc_fid = (SELECT "+branch+"_child FROM "+table+" "
                                         "WHERE ogc_fid = conflict_deleted_fid) "
                    "UNION ALL "
                    # insert new features from theirs
                    "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, "
                    "'modified' AS action, "+cols+" "
                    "FROM "+table+"_diff "+", "+table+"_conflicts_ogc_fid AS cflt "
                    "WHERE ogc_fid = (SELECT "+branch+"_child FROM "+table+"_diff "
                                         "WHERE ogc_fid = conflict_deleted_fid) "
                     # insert deleted features from mine
                    "UNION ALL "
                    "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, "
                    "'deleted' AS action, "+cols+" "
                    "FROM "+table+", "+table+"_conflicts_ogc_fid AS cflt "
                    "WHERE ogc_fid = conflict_deleted_fid "
                    "AND "+branch+"_child IS NULL "
                     # insert deleted features from theirs
                    "UNION ALL "
                    "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, "
                    "'deleted' AS action, "+cols+" "
                    "FROM "+table+"_diff, "+table+"_conflicts_ogc_fid AS cflt "
                    "WHERE ogc_fid = conflict_deleted_fid "
                    "AND "+branch+"_child IS NULL" )
    
                # identify conflicts for deleted
                scur.execute("UPDATE "+table+"_conflicts "
                    "SET conflict_id = ogc_fid "+ "WHERE action = 'deleted'")
    
                # now follow child if any for 'theirs' 'modified' since several
                # edition could be made we want the very last child
                while True:
                    scur.execute("SELECT conflict_id, ogc_fid, "+branch+"_child "
                        "FROM "+table+"_conflicts WHERE origin='theirs' "
                        "AND action='modified' AND "+branch+"_child IS NOT NULL")
                    res = scur.fetchall()
                    if not res :
                        break
                    # replaces each entries by it's child
                    for [cflt_id, fid, child] in res:
                        scur.execute("DELETE FROM "+table+"_conflicts "
                            "WHERE ogc_fid = "+str(fid))
                        scur.execute("INSERT INTO "+table+"_conflicts "
                            "SELECT "+str(cflt_id)+" AS conflict_id, "
                            "'theirs' AS origin, 'modified' AS action, "+cols+" "
                            "FROM "+table+"_diff "
                            "WHERE ogc_fid = "+str(child)+" "
                            "AND "+branch+"_rev_end IS NULL" )
                        scur.execute("INSERT INTO "+table+"_conflicts "
                            "SELECT "+str(cflt_id)+" AS conflict_id, "
                            "'theirs' AS origin, 'deleted' AS action, "+cols+" "
                            "FROM "+table+"_diff "
                            "WHERE ogc_fid = "+str(child)+" "
                            "AND "+branch+"_rev_end IS NOT NULL" )
    
                scur.execute("DELETE FROM geometry_columns "
                    "WHERE f_table_name = '"+table+"_conflicts'")
                for geom_name, srid, geom_type in geoms_name_srid_type: 
                    scur.execute(
                        """SELECT RecoverGeometryColumn(
                        '{table}_conflicts', '{geom_name}', 
                        {srid}, '{geom_type}', 'XY')
                        """.format(table=table, srid=srid, geom_type=geom_type, geom_name=geom_name))
    
    
                scur.execute("CREATE UNIQUE INDEX IF NOT EXISTS "
                    +table+"_conflicts_idx ON "+table+"_conflicts(ogc_fid)")
    
                # create trigers such that on delete the conflict is resolved
                # if we delete 'theirs', we set their child to our fid and
                # their rev_end if we delete 'mine'... well, we delete 'mine'
    
                scur.execute("DROP TRIGGER IF EXISTS delete_"+table+"_conflicts")
                scur.execute("CREATE TRIGGER delete_"+table+"_conflicts "
                "AFTER DELETE ON "+table+"_conflicts\n"
                    "BEGIN\n"
                        "DELETE FROM "+table+" "
                        "WHERE ogc_fid = old.ogc_fid AND old.origin = 'mine';\n"
    
                        "UPDATE "+table+" "
                        "SET "+branch+"_child = (SELECT ogc_fid "
                        "FROM "+table+"_conflicts "
                        "WHERE origin = 'mine' "
                        "AND conflict_id = old.conflict_id), "
                        +branch+"_rev_end = "+str(max_rev)+" "
                        "WHERE ogc_fid = old.ogc_fid AND old.origin = 'theirs';\n"
    
                        "UPDATE "+table+" "
                        "SET "+branch+"_parent = old.ogc_fid "
                        "WHERE ogc_fid = (SELECT ogc_fid "
                        "FROM "+table+"_conflicts WHERE origin = 'mine' "
                        "AND conflict_id = old.conflict_id) "
                        "AND old.origin = 'theirs';\n"
    
                        "DELETE FROM "+table+"_conflicts "
                        "WHERE conflict_id = old.conflict_id;\n"
                    "END")
    
                scur.commit()
    
            scur.execute("CREATE UNIQUE INDEX IF NOT EXISTS "
                +table+"_diff_idx ON "+table+"_diff(ogc_fid)")
            # insert and replace all in diff
            scur.execute("INSERT OR REPLACE INTO "+table+" ("+cols+") "
                "SELECT "+cols+" FROM "+table+"_diff")
    
        scur.commit()
        scur.close()
        
    def __setup_contraint_trigger(self, connection, schema, tables):
        """
        Build and setup unique and foreign key constraints on table views
        """

        # Get unique and foreign key constraints
        (sqlite_filename, pg_conn_info) = connection
        pcur = Db(psycopg2.connect(pg_conn_info))
        pcur.execute("""
        SELECT table_from, columns_from, table_to, columns_to, updtype, deltype
        FROM {schema}.versioning_constraints
        """.format(schema=schema))
       
        tables_wo_schema = [table[1] for table in tables]

        requests = []
            
        # Build trigger upon this contraints and setup on view
        for idx, (table_from, columns_from, table_to, columns_to, updtype, deltype) in enumerate(pcur.fetchall()):

            # table is not being checkout
            if table_from not in tables_wo_schema:
                continue
            
            # unique constraint
            if not table_to:

                for method in ['insert','update']:

                    # check if unique keys already exist
                    when_filter = "(SELECT COUNT(*) FROM {}_view WHERE {}) != 0".format(
                        table_from,
                        " AND ".join(["{0} = NEW.{0}".format(column) for column in columns_from]))

                    # check if unique keys have been modified
                    if method == 'update': 
                        when_filter += " AND " + " AND ".join(["NEW.{0} != OLD.{0}".format(column)
                                                             for column in columns_from]) 

                    keys = ",".join(columns_from)
                    
                    sql = f"""
                    CREATE TRIGGER {method}_check{idx}_unique_{table_from}
                    INSTEAD OF {method} ON {table_from}_view
                    FOR EACH ROW
                    WHEN {when_filter}
                    BEGIN
                    SELECT RAISE(FAIL, "Fail {table_from} {keys} unique constraint");
                    END;"""

                    requests += [sql]

            # foreign key constraint
            else: 

                assert(len(columns_from) == len(columns_to))
                
                # check if referenced keys exists
                when_filter = "(SELECT COUNT(*) FROM {}_view WHERE {}) == 0".format(
                    table_to,
                    " AND ".join(["{} = NEW.{}".format(column_to, column_from)
                                  for column_to, column_from in zip(columns_to, columns_from)]))

                keys = ",".join(columns_from)
                    
                for method in ['insert','update']:
                    
                    sql = f"""
                    CREATE TRIGGER {method}_check{idx}_fkey_{table_from}_to_{table_to}
                    INSTEAD OF {method} ON {table_from}_view
                    FOR EACH ROW
                    WHEN {when_filter}
                    BEGIN
                    SELECT RAISE(FAIL, "Fail {keys} foreign key constraint");
                    END;
                    """

                    requests += [sql]

                # special actions when a referenced key is updated/deleted
                for method in ['delete','update']:

                    # check if referencing keys have been modified
                    when_filter = ""
                    if method == 'update': 
                        when_filter += "WHEN " + " OR ".join(["NEW.{0} != OLD.{0}".format(column)
                                                              for column in columns_to]) 


                    keys_label = ",".join(columns_to) + (" is" if len(columns_to) == 1 else " are")

                    action_type = updtype if method == 'update' else deltype
                    if action_type == 'c':
                        action = ""
                        for column_from, column_to in zip(columns_from, columns_to):
                            where = f"WHERE {column_from} = OLD.{column_to}"
                            if method == 'update':
                                action += f"UPDATE {table_from} SET {column_from} = NEW.{column_to} {where};"""
                            else:
                                action += f"DELETE FROM {table_from} {where};"
                    elif action_type == 'n':
                        pass
                    elif action_type == 'd':
                        pass
                    else:
                        action = f"""SELECT RAISE(FAIL, "{keys_label} still referenced by {table_from}");""";
                    
                    sql = f"""
                    CREATE TRIGGER {method}_check{idx}_fkey_modifed_{table_from}_to_{table_to}
                    INSTEAD OF {method} ON {table_to}_view
                    FOR EACH ROW
                    {when_filter}
                    BEGIN
                    {action}
                    END;
                    """

                    requests += [sql]
                    
        scur = Db(dbapi2.connect(sqlite_filename))
        for request in requests:
            scur.execute(request)

        scur.commit()
            
        scur.close()
        pcur.close()

    def __get_checkout_tables(self, connection, table_names):
        """
        Build and return tables to be checkout according to given pg_tables parameter. 
        It also adds the referenced table (in order to check the foreign key)
        :returns: a list of tuple (schema, table, branch)
        """
        (sqlite_filename, pg_conn_info) = connection
        pcur = Db(psycopg2.connect(pg_conn_info))

        # We use and ordered dict because we don't want table duplicate and we want to keep original
        # order for later purpose (see selectedFeatureList in checkout method)
        tables = OrderedDict()
        for table_name in table_names:
            schema, table = table_name.split('.')
            if not ( schema and table and schema[-9:] == "_rev_head"):
                raise RuntimeError("Schema names must end with "
                    "suffix _branch_rev_head")

            schema, _, branch = schema[:-9].rpartition('_')
            tables[(schema, table, branch)] = None

            # Search for referenced table
            sql = """
            SELECT DISTINCT table_to 
            FROM {schema}.versioning_constraints
            WHERE table_from = '{table}'
            AND table_to IS NOT NULL;
            """.format(schema=schema, table=table)

            pcur.execute(sql)

            # add them (if not already added). We don't use set
            # because we want to keep the original order
            for ref_table in pcur.fetchall():
                tables[(schema, ref_table[0], branch)] = None

        return list(tables)
        
    def checkout(self, connection, pg_table_names, selected_feature_lists = []):
        (sqlite_filename, pg_conn_info) = connection
        """create working copy from versioned database tables
        pg_table_names must be complete schema.table names
        the schema name must end with _branch_rev_head
        the file sqlite_filename must not exists
        the views and trigger for local edition will be created
        along with the tables and triggers for conflict resolution"""

        if os.path.isfile(sqlite_filename):
            raise RuntimeError("File "+sqlite_filename+" already exists")

        tables = self.__get_checkout_tables(connection, pg_table_names)
        pcur = Db(psycopg2.connect(pg_conn_info))
    
        temp_view_names = []
        first_table = True
        for (schema, table, branch), feature_list in list(zip_longest(tables, selected_feature_lists)):
    
            # fetch the current rev
            pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
            current_rev = int(pcur.fetchone()[0])
    
            # max pkey for this table
            pkey = pg_pk( pcur, schema, table )
            pcur.execute("SELECT MAX("+pkey+") FROM "+schema+"."+table)
            [max_pg_pk] = pcur.fetchone()
            if not max_pg_pk :
                max_pg_pk = 0
    
            temp_view_name = schema+"."+table+"_checkout_temp_view"
            temp_view_names.append(temp_view_name)
            # use ogr2ogr to create spatialite db
            pgeom = pg_geom(pcur, schema, table)
            if first_table:
                first_table = False
                cmd = ['ogr2ogr',
                        '-f', 'SQLite',
                       '-lco', 'GEOMETRY_NAME={}'.format(pgeom),
                        '-dsco', 'SPATIALITE=yes',
                        '"' + sqlite_filename + '"',
                        'PG:"'+pg_conn_info+'"', temp_view_name,
                        '-nln', table]
                # We need to create a temp view because of windows commandline
                # limitations, e.g. ogr2ogr with a very long where clause
                # GDAL > 2.1 allows specifying a filename for where args, e.g.
                #cmd += ['-where', '"'+pkey+' in ('+",".join([str(feature_list[i]) for i in range(0, len(feature_list))])+')"']
                # Get column names because we cannot just call 'SELECT *'
                pcur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = \'"+schema+"\' AND table_name   = \'"+table+"\'")
                column_list = pcur.fetchall()
                new_columns_str = preserve_fid( pkey, column_list)
                view_str = "CREATE OR REPLACE VIEW "+temp_view_name+" AS SELECT "+new_columns_str+" FROM " +schema+"."+table
                if feature_list:
                    view_str = "CREATE OR REPLACE VIEW "+temp_view_name+" AS SELECT "+new_columns_str+" FROM " +schema+"."+table+" WHERE "+pkey+' in ('+",".join([str(feature_list[i]) for i in range(0, len(feature_list))])+')'
                pcur.execute(view_str)
                pcur.commit()
    
                if DEBUG: print(' '.join(cmd))
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
                       '-f', 'SQLite',
                       '-lco', 'GEOMETRY_NAME={}'.format(pgeom),
                       '-update',
                       '"' + sqlite_filename + '"',
                       'PG:"'+pg_conn_info+'"', temp_view_name,
                       '-nln', table]
                # Same comments as in 'if feature_list' above
                pcur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = \'"+schema+"\' AND table_name   = \'"+table+"\'")
                column_list = pcur.fetchall()
                new_columns_str = preserve_fid( pkey, column_list)
                view_str = "CREATE OR REPLACE VIEW "+temp_view_name+" AS SELECT "+new_columns_str+" FROM " +schema+"."+table
                if feature_list:
                    view_str = "CREATE OR REPLACE VIEW "+temp_view_name+" AS SELECT "+new_columns_str+" FROM " +schema+"."+table+" WHERE "+pkey+' in ('+",".join([str(feature_list[i]) for i in range(0, len(feature_list))])+')'
                pcur.execute(view_str)
                pcur.commit()
    
                if DEBUG: print(' '.join(cmd))
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
            
            cols = ""
            newcols = ""
            hcols = ['ogc_fid'] + sum([[brch+'_rev_begin', brch+'_rev_end',
                    brch+'_parent', brch+'_child'] for brch in pg_branches( pcur, schema ) ],[])
            for res in scur.execute("PRAGMA table_info("+table+")").fetchall():
                if res[1].lower() not in [c.lower() for c in hcols]:
                    cols += quote_ident(res[1]) + ", "
                    newcols += "new."+quote_ident(res[1])+", "
            cols = cols[:-2]
            newcols = newcols[:-2] # remove last coma
    
            scur.execute( "CREATE VIEW "+table+"_view "+"AS "
                "SELECT ROWID AS ROWID, ogc_fid, "+cols+" "
                "FROM "+table+" WHERE "+branch+"_rev_end IS NULL "
                "AND "+branch+"_rev_begin IS NOT NULL")
    
            max_fid_sub = ("( SELECT MAX(max_fid) FROM ( SELECT MAX(ogc_fid) AS "
                "max_fid FROM "+table+" UNION SELECT max_pk AS max_fid "
                "FROM initial_revision WHERE table_name = '"+table+"') )")
            current_rev_sub = ("(SELECT rev FROM initial_revision "
                "WHERE table_name = '"+table+"')")
    
            scur.execute("DELETE FROM views_geometry_columns "
                "WHERE view_name = '"+table+"_view'")

            if pgeom in cols:
                scur.execute("""INSERT INTO views_geometry_columns
                (view_name, view_geometry, view_rowid, 
                f_table_name, f_geometry_column, read_only)
                VALUES ('{0}_view', '{1}', 'rowid', '{0}', '{1}', 0)""".format(table, pgeom))
    
            # when we edit something old, we insert and update parent
            scur.execute(
            "CREATE TRIGGER update_old_"+table+" "
                "INSTEAD OF UPDATE ON "+table+"_view "
                "WHEN (SELECT COUNT(*) FROM "+table+" "
                "WHERE ogc_fid = new.ogc_fid "
                "AND ("+branch+"_rev_begin <= "+current_rev_sub+" ) ) \n"
                "BEGIN\n"
                "INSERT INTO "+table+" "
                "(ogc_fid, "+cols+", "+branch+"_rev_begin, "
                 +branch+"_parent) "
                "VALUES "
                "("+max_fid_sub+"+1, "+newcols+", "+current_rev_sub+"+1, "
                  "old.ogc_fid);\n"
                "UPDATE "+table+" SET "+branch+"_rev_end = "+current_rev_sub+", "
                +branch+"_child = "+max_fid_sub+" WHERE ogc_fid = old.ogc_fid;\n"
                "END")
            # when we edit something new, we just update
            scur.execute("CREATE TRIGGER update_new_"+table+" "
            "INSTEAD OF UPDATE ON "+table+"_view "
                  "WHEN (SELECT COUNT(*) FROM "+table+" "
                  "WHERE ogc_fid = new.ogc_fid AND ("+branch+"_rev_begin > "
                  +current_rev_sub+" ) ) \n"
                  "BEGIN\n"
                    "REPLACE INTO "+table+" "
                    "(ogc_fid, "+cols+", "+branch+"_rev_begin, "+branch+"_parent) "
                    "VALUES "
                    "(new.ogc_fid, "+newcols+", "+current_rev_sub+"+1, (SELECT "
                    +branch+"_parent FROM "+table+
                    " WHERE ogc_fid = new.ogc_fid));\n"
                  "END")
    
            scur.execute("CREATE TRIGGER insert_"+table+" "
            "INSTEAD OF INSERT ON "+table+"_view\n"
                "BEGIN\n"
                    "INSERT INTO "+table+" "+
                    "(ogc_fid, "+cols+", "+branch+"_rev_begin) "
                    "VALUES "
                    "("+max_fid_sub+"+1, "+newcols+", "+current_rev_sub+"+1);\n"
                "END")
            
            scur.execute("CREATE TRIGGER delete_"+table+" "
            "INSTEAD OF DELETE ON "+table+"_view\n"
                "BEGIN\n"
                  # update it if its old
                    "UPDATE "+table+" "
                        "SET "+branch+"_rev_end = "+current_rev_sub+" "
                        "WHERE ogc_fid = old.ogc_fid "
                        "AND "+branch+"_rev_begin < "+current_rev_sub+"+1;\n"
                  # update its parent if its modified
                    "UPDATE "+table+" "
                        "SET "+branch+"_rev_end = "+current_rev_sub+", "+branch+"_child = NULL "
                        "WHERE "+branch+"_child = old.ogc_fid;\n"
                  # delete it if its new and remove it from child
                    "UPDATE "+table+" "
                        "SET "+branch+"_child = NULL "
                        "WHERE "+branch+"_child = old.ogc_fid "
                        "AND "+branch+"_rev_begin = "+current_rev_sub+"+1;\n"
                    "DELETE FROM "+table+" "
                        "WHERE ogc_fid = old.ogc_fid "
                        "AND "+branch+"_rev_begin = "+current_rev_sub+"+1;\n"
                "END")
    
            scur.commit()
            scur.close()
        # Remove temp views after sqlite file is written
        for i in temp_view_names:
            del_view_str = "DROP VIEW IF EXISTS " + i
            pcur.execute(del_view_str)
            pcur.commit()
        pcur.close()

        self.__setup_contraint_trigger(connection, schema, tables)
        
    
    def unresolved_conflicts(self, connection):
        sqlite_filename = connection[0]
        """return a list of tables with unresolved conflicts"""
        found = []
        scur = Db(dbapi2.connect(sqlite_filename))
        scur.execute("SELECT tbl_name FROM sqlite_master "
            "WHERE type='table' AND tbl_name LIKE '%_conflicts'")
        for table_conflicts in scur.fetchall():
            if DEBUG: print('table_conflicts:', table_conflicts[0])
            scur.execute("SELECT * FROM "+table_conflicts[0])
            if scur.fetchone():
                found.append( table_conflicts[0][:-10] )
        scur.commit()
        scur.close()
        return found
    
    def commit(self, connection, commit_msg, commit_user = ''):
        """merge modifications into database
        returns the number of updated layers"""
        # get the target revision from the spatialite db
        # create the diff in postgres
        # load the diff in spatialite
        # detect conflicts
        # merge changes and update target_revision
        # delete diff
        (sqlite_filename, pg_conn_info) = connection
        unresolved = self.unresolved_conflicts([sqlite_filename])
        if unresolved:
            raise RuntimeError("There are unresolved conflicts in "
                +sqlite_filename+" for table(s) "+', '.join(unresolved) )
    
        late_by = self.late([sqlite_filename, pg_conn_info])
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
            sql = sql.replace(table, table+"_diff", 1)
            scur.execute(sql)
            scur.execute("DELETE FROM geometry_columns "
                "WHERE f_table_name = '"+table+"_diff'")
            scur.execute("""
                INSERT INTO geometry_columns
                SELECT '{table}_diff', f_geometry_column, geometry_type,
                coord_dimension, srid, spatial_index_enabled
                FROM geometry_columns WHERE f_table_name = '{table}'
                """.format(table=table))
            scur.execute( "INSERT INTO "+table+"_diff "
                    "SELECT * "
                    "FROM "+table+" "
                    "WHERE "+branch+"_rev_end = "+str(rev)+" "
                    "OR "+branch+"_rev_begin > "+str(rev))
            scur.execute( "SELECT ogc_fid FROM "+table+"_diff")
            there_is_something_to_commit = scur.fetchone()
            if DEBUG: print("there_is_something_to_commit ", there_is_something_to_commit)
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
                   '-lco', 'GEOMETRY_NAME={}'.format(pgeom),
                    '-f',
                    'PostgreSQL',
                    'PG:"'+pg_conn_info+'"',
                    '-lco',
                    'FID='+pkey,
                    '"' + sqlite_filename + '"',
                    table+"_diff",
                    '-nln', diff_schema+'.'+table+"_diff"]
    
            if DEBUG: print(' '.join(cmd))
            os.system(' '.join(cmd))
    
            for l in pcur.execute( "select * from geometry_columns").fetchall():
                if DEBUG: print(l)
    
            # remove dif table and geometry column
            scur.execute("DELETE FROM geometry_columns "
                "WHERE f_table_name = '"+table+"_diff'")
            scur.execute("DROP TABLE "+table+"_diff")
    
    
            if not there_is_something_to_commit:
                if DEBUG: print("nothing to commit for ", table)
                pcur.close()
                continue
    
            nb_of_updated_layer += 1
    
            pcur.execute("SELECT rev FROM "+table_schema+".revisions "
                "WHERE rev = "+str(rev+1))
            if not pcur.fetchone():
                if DEBUG: print("inserting rev ", str(rev+1))
                pcur.execute("INSERT INTO "+table_schema+".revisions "
                    "(rev, commit_msg, branch, author) "
                    "VALUES ("+str(rev+1)+", '"+escape_quote(commit_msg)+"', '"+branch+"',"
                    "'"+os_info()+":"+get_username()+"."+pg_username+"."+commit_user+"')")
    
            other_branches = pg_branches( pcur, table_schema ).remove(branch)
            other_branches = other_branches if other_branches else []
            other_branches_columns = sum([
                [brch+'_rev_begin', brch+'_rev_end',
                brch+'_parent', brch+'_child']
                for brch in other_branches], [])
            pcur.execute("""
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = '{table_schema}'
                AND table_name = '{table}'
                """.format(table_schema=table_schema, table=table))
            cols = ""
            cols_cast = ""
            for col in pcur.fetchall():
                if col[0] not in other_branches_columns:
                    cols += quote_ident(col[0])+", "
                    if col[1] != 'ARRAY':
                        if col[1] == 'USER-DEFINED':
                            cast = ""
                        elif col[1] == 'character' and col[2]:
                            cast = "::varchar"
                        else:
                            cast = "::"+col[1] 
                        cols_cast += quote_ident(col[0])+cast+", "
                    else :
                        cols_cast += ("regexp_replace(regexp_replace("
                                +col[0]+"::varchar,'^\(.*:','{'),'\)$','}')::"
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
        for schema, conn_info in schema_list.items():
            pcur = Db(psycopg2.connect(conn_info))
            pcur.execute("DROP SCHEMA "+schema+" CASCADE")
            pcur.commit()
            pcur.close()
    
        return nb_of_updated_layer
