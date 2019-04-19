# -*- coding: utf-8 -*-

from __future__ import absolute_import
from .utils import (Db, pg_pk, pg_geom, pg_geoms, pg_branches, quote_ident,
                    preserve_fid, escape_quote, get_username, os_info,
                    get_checkout_tables, get_pkey)
from .constraints import ConstraintBuilder, check_unique_constraints

import psycopg2
import tempfile
import os

DEBUG = False


class pgVersioningLocal(object):
    def __pragmaTableInfo(self, schema, table):
        """returns an sql query to fetch information like PRAGMA table_info(table) from SQLite"""
        sql = """select 
          ordinal_position, 
          column_name, 
          data_type, 
          is_nullable, 
          column_default, 
          COALESCE(pk.pk, false) as pk 
        from 
          INFORMATION_SCHEMA.COLUMNS 
          LEFT JOIN (
            SELECT 
              a.attname, 
              true AS pk 
            FROM 
              pg_index i 
              JOIN pg_attribute a ON a.attrelid = i.indrelid 
              AND a.attnum = ANY(i.indkey) 
            WHERE 
              i.indrelid = '{schema}.{table}' :: regclass 
              AND i.indisprimary
          ) pk ON pk.attname = column_name 
        WHERE 
          (table_schema, table_name) = ('{schema}', '{table}') 
        ORDER BY 
          ordinal_position""".format(schema=schema, table=table)

        return sql

    def revision(self, connection):
        (pg_conn_info, wcs, pg_conn_info_copy) = connection
        """returns the revision the working copy was created from plus one"""
        pcurcpy = Db(psycopg2.connect(pg_conn_info_copy))
        pcurcpy.execute("SELECT rev " + "FROM "+wcs+".initial_revision")
        rev = 0
        for [res] in pcurcpy.fetchall():
            if rev:
                assert(res == rev)
            else:
                rev = res
        pcurcpy.close()
        return rev + 1

    def late(self, connection):
        (pg_conn_info, wcs, pg_conn_info_copy) = connection
        """Return 0 if up to date, the number of commits in between otherwise"""
        pcurcpy = Db(psycopg2.connect(pg_conn_info_copy))
        pcur = Db(psycopg2.connect(pg_conn_info))
        pcurcpy.execute("SELECT rev, branch, table_schema "
                        "FROM "+wcs+".initial_revision")
        versioned_layers = pcurcpy.fetchall()
        if not versioned_layers:
            raise RuntimeError("Cannot find versioned layer in "
                               + wcs)

        late_by = 0

        for [rev, branch, table_schema] in versioned_layers:
            pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions "
                         "WHERE branch = '"+branch+"'")
            [max_rev] = pcur.fetchone()
            late_by = max(max_rev - rev, late_by)

        pcurcpy.close()
        pcur.close()
        return late_by

    def update(self, connection):
        (pg_conn_info, wcs, pg_conn_info_copy) = connection
        """merge modifications since last update into working copy"""
        if DEBUG:
            print("update")
        if self.unresolved_conflicts([pg_conn_info_copy, wcs, pg_conn_info_copy]):
            raise RuntimeError("There are unresolved conflicts in "
                               + wcs)
        # get the target revision from the postgresql copy db
        # create the diff in postgres
        # load the diff in postgresql copy
        # detect conflicts and create conflict layers
        # merge changes and update target_revision
        # delete diff

        pcurcpy = Db(psycopg2.connect(pg_conn_info_copy))
        pcurcpy.execute("SELECT rev, branch, table_schema, table_name, max_pk "
                        "FROM {}.initial_revision".format(wcs))
        versioned_layers = pcurcpy.fetchall()

        for [rev, branch, table_schema, table, current_max_pk] in versioned_layers:
            pcur = Db(psycopg2.connect(pg_conn_info))
            pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions "
                         "WHERE branch = '"+branch+"'")
            [max_rev] = pcur.fetchone()
            if max_rev == rev:
                if DEBUG:
                    print("Nothing new in branch "+branch+" in "+table_schema+"."
                          + table+" since last update")
                pcur.close()
                continue

            # get the max pkey
            pkey = pg_pk(pcur, table_schema, table)
            pgeom = pg_geom(pcur, table_schema, table)
            pgeoms = pg_geoms(pcur, table_schema, table)
            pcur.execute("SELECT MAX("+pkey+") FROM "+table_schema+"."+table)
            [max_pg_pk] = pcur.fetchone()
            if not max_pg_pk:
                max_pg_pk = 0

            # create the diff
            diff_schema = (table_schema+"_"+branch+"_"+str(rev) +
                           "_to_"+str(max_rev)+"_diff")
            pcur.execute("SELECT schema_name FROM information_schema.schemata "
                         "WHERE schema_name = '"+diff_schema+"'")
            if not pcur.fetchone():
                pcur.execute("CREATE SCHEMA "+diff_schema)

            other_branches = pg_branches(pcur, table_schema).remove(branch)
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
            cols = cols[:-2]  # remove last coma and space
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

            pcur.execute("DROP TABLE IF EXISTS "+diff_schema+"."+table+"_diff")
            pcur.execute("CREATE TABLE "+diff_schema+"."+table+"_diff AS "
                         "SELECT "+cols+geom+" "
                         "FROM "+table_schema+"."+table+" "
                         "WHERE "+branch+"_rev_end >= "+str(rev)+" "
                         "OR "+branch+"_rev_begin > "+str(rev))
            pcur.execute("ALTER TABLE "+diff_schema+"."+table+"_diff "
                         "ADD CONSTRAINT "+table+"_"+branch+"_pk_pk "
                         "PRIMARY KEY ("+pkey+")")
            pcur.commit()

            pcurcpy.execute("DROP TABLE IF EXISTS "+wcs+"."+table+"_diff")
            pcurcpy.execute("DROP TABLE IF EXISTS idx_" +
                            wcs+"."+table+"_diff_GEOMETRY")
            pcurcpy.execute("DELETE FROM geometry_columns "
                            "WHERE f_table_name = '"+table+"_diff'")
            pcurcpy.commit()

            # import the diff to postgresql
            pgeom = pg_geom(pcur, table_schema, table)
            cmd = ['ogr2ogr',
                   '-preserve_fid',
                   '-lco', 'FID=ogc_fid',
                   '-lco', 'schema=' + wcs,
                   '-lco', 'GEOMETRY_NAME={}'.format(pgeom),
                   '-f', 'PostgreSQL',
                   '-update',
                   'PG:"'+pg_conn_info_copy+'"',
                   'PG:"'+pg_conn_info+'"',
                   diff_schema+'.'+table+"_diff",
                   '-nln', wcs+'.'+table+"_diff"]

            if DEBUG:
                print(' '.join(cmd))
            os.system(' '.join(cmd))

            # cleanup in postgis
            pcur.execute("DROP SCHEMA "+diff_schema+" CASCADE")
            pcur.commit()
            pcur.close()

            pcurcpy.execute(self.__pragmaTableInfo(wcs, table))
            cols = ""
            for col in pcurcpy.fetchall():
                cols += quote_ident(col[1])+", "
            cols = cols[:-2]  # remove last coma and space

            sql = """UPDATE {wcs}.initial_revision 
                  SET rev = {max_rev}
                  , max_pk = {max_pg_pk} 
                  WHERE table_schema = '{wcs_short}' 
                  and table_name = '{table}'""".format(
                          wcs=wcs,
                          wcs_short=wcs[:-len("_trunk_rev_head")],
                          max_rev=max_rev, max_pg_pk=max_pg_pk,
                          table=table)
            print(sql)
            # update the initial revision
            pcurcpy.execute(sql)

            pcurcpy.execute("UPDATE "+wcs+"."+table+" "
                            "SET "+branch+"_rev_end = "+str(max_rev)+" "
                            "WHERE "+branch+"_rev_end = "+str(rev))
            pcurcpy.execute("UPDATE "+wcs+"."+table+" "
                            "SET "+branch+"_rev_begin = "+str(max_rev+1)+" "
                            "WHERE "+branch+"_rev_begin = "+str(rev+1))

            # we cannot add constrain to the spatialite db in order to have
            # spatialite update parent and child when we bump inserted pkey
            # above the max pkey in the diff we must do this manually
            bump = max_pg_pk - current_max_pk
            assert(bump >= 0)
            # now bump the pks of inserted rows in working copy
            # note that to do that, we need to set a negative value because
            # the UPDATE is not implemented correctly according to:
            # http://stackoverflow.com/questions/19381350/simulate-order-by-in-sqlite-update-to-handle-uniqueness-constraint
            pcurcpy.execute("UPDATE "+wcs+"."+table+" "
                            "SET ogc_fid = -ogc_fid  "
                            "WHERE "+branch+"_rev_begin = "+str(max_rev+1))
            pcurcpy.execute("UPDATE "+wcs+"."+table+" "
                            "SET ogc_fid = "+str(bump)+"-ogc_fid WHERE ogc_fid < 0")
            # and bump the pkey in the child field
            # not that we don't care for nulls since adding something
            # to null is null
            pcurcpy.execute("UPDATE "+wcs+"."+table+" "
                            "SET "+branch+"_child = "+branch +
                            "_child  + "+str(bump)+" "
                            "WHERE "+branch+"_rev_end = "+str(max_rev))

            # detect conflicts: conflict occur if two lines with the same pkey have
            # been modified (i.e. have a non null child) or one has been removed
            # and the other modified
            pcurcpy.execute("DROP VIEW  IF EXISTS "+wcs +
                            "."+table+"_conflicts_ogc_fid")
            pcurcpy.execute("CREATE VIEW "+wcs+"."+table+"_conflicts_ogc_fid AS "
                            "SELECT DISTINCT sl.ogc_fid as conflict_deleted_fid "
                            "FROM "+wcs+"."+table+" AS sl, "+wcs+"."+table+"_diff AS pg "
                            "WHERE sl.ogc_fid = pg.ogc_fid "
                            "AND sl."+branch+"_child != pg."+branch+"_child")
            pcurcpy.execute("SELECT conflict_deleted_fid "
                            "FROM  "+wcs+"."+table+"_conflicts_ogc_fid")
            if pcurcpy.fetchone():
                if DEBUG:
                    print("there are conflicts")
                # add layer for conflicts
                pcurcpy.execute("DROP TABLE IF EXISTS " +
                                wcs+"."+table+"_conflicts ")
                pcurcpy.execute("CREATE TABLE "+wcs+"."+table+"_conflicts AS "
                                # insert new features from mine
                                "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, "
                                "'modified' AS action, "+cols+" "
                                "FROM "+wcs+"."+table+", "+wcs+"."+table+"_conflicts_ogc_fid AS cflt "
                                "WHERE ogc_fid = (SELECT "+branch + \
                                "_child FROM "+wcs+"."+table+" "
                                "WHERE ogc_fid = conflict_deleted_fid) "
                                "UNION ALL "
                                # insert new features from theirs
                                "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, "
                                "'modified' AS action, "+cols+" "
                                "FROM "+wcs+"."+table+"_diff "+", "+wcs+"."+table+"_conflicts_ogc_fid AS cflt "
                                "WHERE ogc_fid = (SELECT "+branch + \
                                "_child FROM "+wcs+"."+table+"_diff "
                                "WHERE ogc_fid = conflict_deleted_fid) "
                                # insert deleted features from mine
                                "UNION ALL "
                                "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, "
                                "'deleted' AS action, "+cols+" "
                                "FROM "+wcs+"."+table+", "+wcs+"."+table+"_conflicts_ogc_fid AS cflt "
                                "WHERE ogc_fid = conflict_deleted_fid "
                                "AND "+branch+"_child IS NULL "
                                # insert deleted features from theirs
                                "UNION ALL "
                                "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, "
                                "'deleted' AS action, "+cols+" "
                                "FROM "+wcs+"."+table+"_diff, "+wcs+"."+table+"_conflicts_ogc_fid AS cflt "
                                "WHERE ogc_fid = conflict_deleted_fid "
                                "AND "+branch+"_child IS NULL")

                # identify conflicts for deleted
                pcurcpy.execute("UPDATE "+wcs+"."+table+"_conflicts "
                                "SET conflict_id = ogc_fid " + "WHERE action = 'deleted'")

                # now follow child if any for 'theirs' 'modified' since several
                # edition could be made we want the very last child
                while True:
                    pcurcpy.execute("SELECT conflict_id, ogc_fid, "+branch+"_child "
                                    "FROM "+wcs+"."+table+"_conflicts WHERE origin='theirs' "
                                    "AND action='modified' AND "+branch+"_child IS NOT NULL")
                    res = pcurcpy.fetchall()
                    if not res:
                        break
                    # replaces each entries by it's child
                    for [cflt_id, fid, child] in res:
                        pcurcpy.execute("DELETE FROM "+wcs+"."+table+"_conflicts "
                                        "WHERE ogc_fid = "+str(fid))
                        pcurcpy.execute("INSERT INTO "+wcs+"."+table+"_conflicts "
                                        "SELECT "+str(cflt_id) +
                                        " AS conflict_id, "
                                        "'theirs' AS origin, 'modified' AS action, "+cols+" "
                                        "FROM "+wcs+"."+table+"_diff "
                                        "WHERE ogc_fid = "+str(child)+" "
                                        "AND "+branch+"_rev_end IS NULL")
                        pcurcpy.execute("INSERT INTO "+wcs+"."+table+"_conflicts "
                                        "SELECT "+str(cflt_id) +
                                        " AS conflict_id, "
                                        "'theirs' AS origin, 'deleted' AS action, "+cols+" "
                                        "FROM "+wcs+"."+table+"_diff "
                                        "WHERE ogc_fid = "+str(child)+" "
                                        "AND "+branch+"_rev_end IS NOT NULL")

                pcurcpy.execute("CREATE UNIQUE INDEX IF NOT EXISTS "
                                + table+"_conflicts_idx ON "+wcs+"."+table+"_conflicts(ogc_fid)")

                # create trigers such that on delete the conflict is resolved
                # if we delete 'theirs', we set their child to our fid and
                # their rev_end if we delete 'mine'... well, we delete 'mine'
                pcurcpy.execute("CREATE OR REPLACE FUNCTION "
                                + wcs+".delete_"+table+"_conflicts() RETURNS trigger AS $$\n"
                                "BEGIN\n"
                                "DELETE FROM "+wcs+"."+table+" "
                                "WHERE ogc_fid = old.ogc_fid AND old.origin = 'mine';\n"

                                "UPDATE "+wcs+"."+table+" "
                                "SET "+branch+"_child = (SELECT ogc_fid "
                                "FROM "+wcs+"."+table+"_conflicts "
                                "WHERE origin = 'mine' "
                                "AND conflict_id = old.conflict_id), "
                                + branch+"_rev_end = "+str(max_rev)+" "
                                "WHERE ogc_fid = old.ogc_fid AND old.origin = 'theirs';\n"

                                "UPDATE "+wcs+"."+table+" "
                                "SET "+branch+"_parent = old.ogc_fid "
                                "WHERE ogc_fid = (SELECT ogc_fid "
                                "FROM "+wcs+"."+table+"_conflicts WHERE origin = 'mine' "
                                "AND conflict_id = old.conflict_id) "
                                "AND old.origin = 'theirs';\n"

                                "DELETE FROM "+wcs+"."+table+"_conflicts "
                                "WHERE conflict_id = old.conflict_id;\n"
                                "END;\n"
                                "$$ LANGUAGE plpgsql;")
                pcurcpy.execute("DROP TRIGGER IF EXISTS "
                                "delete_"+table+"_conflicts ON "+wcs+"."+table+"_conflicts ")
                pcurcpy.execute("CREATE TRIGGER "
                                "delete_"+table+"_conflicts "
                                "INSTEAD OF DELETE ON "+wcs+"."+table+"_conflicts "
                                "FOR EACH ROW "
                                "EXECUTE PROCEDURE "+wcs+".delete_"+table+"_conflicts();")
                pcurcpy.commit()

            pcurcpy.execute("CREATE UNIQUE INDEX IF NOT EXISTS "
                            + table+"_diff_idx ON "+wcs+"."+table+"_diff(ogc_fid)")
            
            # insert and replace all in diff
            pkey = pg_pk(pcurcpy, wcs, table)
            
            pcurcpy.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{table_schema}'
                AND table_name = '{table}'
                """.format(table_schema=wcs, table=table))
            history_columns = [pkey]
            cols = ""
            coli = ""
            allcols = pcurcpy.fetchall()
            for [col] in allcols:
                if col not in history_columns:
                    cols = quote_ident(col)+", "+cols
                    coli = quote_ident(col)+", "+coli
                else:
                    coli = quote_ident(col)+", "+coli
                    cols = "(SELECT max({pkey}) FROM {table_schema}.{table}) + row_number() over() as ".format(table_schema=wcs,
                                                           table=table, pkey=pkey)+quote_ident(pkey)+", "+cols
                    
            cols = cols[:-2] # remove last coma and space
            coli = coli[:-2]
            pcurcpy.execute("INSERT INTO "+wcs+"."+table+" ("+coli+") "
                            "SELECT "+cols+" FROM "+wcs+"."+table+"_diff")

        pcurcpy.commit()
        pcurcpy.close()

    def checkout(self, connection, pg_table_names, selected_feature_lists=[]):
        (pg_conn_info, wcs, pg_conn_info_copy) = connection
        """create working copy from versioned database tables
        pg_table_names must be complete schema.table names
        the schema name must end with _branch_rev_head
        the views and trigger for local edition will be created
        along with the tables and triggers for conflict resolution"""

        tables = get_checkout_tables(pg_conn_info, pg_table_names,
                                     selected_feature_lists)

        pcur = Db(psycopg2.connect(pg_conn_info))
        pcurcpy = Db(psycopg2.connect(pg_conn_info_copy))

        pcurcpy.execute("CREATE SCHEMA " + wcs)
        pcurcpy.commit()

        temp_view_names = []
        first_table = True
        for (schema, table, branch), feature_list in tables.items():

            constraint_builder = ConstraintBuilder(pcur, pcurcpy, schema, wcs)

            # fetch the current rev
            pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
            current_rev = int(pcur.fetchone()[0])

            # max pkey for this table
            pkey = pg_pk(pcur, schema, table)
            pcur.execute("SELECT MAX("+pkey+") FROM "+schema+"."+table)
            [max_pg_pk] = pcur.fetchone()
            if not max_pg_pk:
                max_pg_pk = 0

            temp_view_name = schema+"."+table+"_checkout_temp_view"
            temp_view_names.append(temp_view_name)

            # export schema and tables to the database
            tmp_dir = tempfile.gettempdir()
            tmp_dump = os.path.join(tmp_dir, "versioning.sql")
            # use ogr2ogr to create the dump
            if first_table:
                first_table = False
                # We use the same logic as spatialite
                # TODO: improve postgresql logic
                pcur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = \'" +
                             schema+"\' AND table_name   = \'"+table+"\'")
                column_list = pcur.fetchall()
                new_columns_str = preserve_fid(pkey, column_list)
                view_str = f"""
                CREATE OR REPLACE VIEW {temp_view_name} AS
                SELECT {new_columns_str} FROM {schema}.{table}"""
                if feature_list:
                    actual_table_pk = get_pkey(pcur, schema, table)
                    fids_str = ",".join([str(feature_list[i]) for i in range(0, len(feature_list))])
                    view_str += f" WHERE {actual_table_pk} in ({fids_str})"
                pcur.execute(view_str)
                pcur.commit()

                pgeom = pg_geom(pcur, schema, table)
                cmd = ['ogr2ogr',
                       '-lco', 'schema=' + wcs,
                       '-lco', 'DROP_TABLE=OFF',
                       '-lco', 'GEOMETRY_NAME={}'.format(pgeom),
                       '-f', 'PGDump',
                       '"' + tmp_dump + '"',
                       'PG:"'+pg_conn_info+' tables=' + wcs + '.' + table + '"', temp_view_name,
                       '-nln', table]

                if DEBUG:
                    print(' '.join(cmd))
                
                os.system(' '.join(cmd))
                pcurcpy = Db(psycopg2.connect(pg_conn_info_copy))
                pcurcpy.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                pcurcpy.execute(open(tmp_dump, "r").read().replace(
                    "CREATE SCHEMA", "CREATE SCHEMA IF NOT EXISTS"))
                pcurcpy.commit()

                # save target revision in a table
                pcurcpy.execute("CREATE TABLE "+wcs+".initial_revision AS SELECT " +
                                str(current_rev)+" AS rev, '" +
                                str(branch)+"'::text AS branch, '" +
                                str(schema)+"'::text AS table_schema, '" +
                                str(table)+"'::text AS table_name, " +
                                str(max_pg_pk)+" AS max_pk")
                pcurcpy.commit()

            else:
                # Same comments as in 'if feature_list' above
                pcur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = \'" +
                             schema+"\' AND table_name   = \'"+table+"\'")
                column_list = pcur.fetchall()
                new_columns_str = preserve_fid(pkey, column_list)
                view_str = "CREATE OR REPLACE VIEW "+temp_view_name + \
                    " AS SELECT "+new_columns_str+" FROM " + schema+"."+table
                if feature_list:
                    view_str = "CREATE OR REPLACE VIEW "+temp_view_name+" AS SELECT "+new_columns_str+" FROM " + schema+"." + \
                        table+" WHERE "+pkey + \
                        ' in ('+",".join([str(feature_list[i])
                                          for i in range(0, len(feature_list))])+')'
                pcur.execute(view_str)
                pcur.commit()

                pgeom = pg_geom(pcur, schema, table)
                cmd = ['ogr2ogr',
                       '-lco', 'schema=' + wcs,
                       '-lco', 'DROP_TABLE=OFF',
                       '-lco', 'GEOMETRY_NAME={}'.format(pgeom),
                       '-f', 'PGDump',
                       '"' + tmp_dump + '"',
                       'PG:"'+pg_conn_info+' tables=' + wcs + '.' + table + '"', temp_view_name,
                       '-nln', table]
                
                if DEBUG:
                    print(' '.join(cmd))
                os.system(' '.join(cmd))
                pcurcpy = Db(psycopg2.connect(pg_conn_info_copy))
                pcurcpy.execute(open(tmp_dump, "r").read().replace(
                    "CREATE SCHEMA", "CREATE SCHEMA IF NOT EXISTS"))
                pcurcpy.commit()

                # save target revision in a table if not in there
                pcurcpy.execute("INSERT INTO "+wcs+".initial_revision"
                                "(rev, branch, table_schema, table_name, max_pk) "
                                "VALUES ("+str(current_rev)+", '"+branch+"', '" +
                                schema+"', '"+table+"', "+str(max_pg_pk)+")")
                pcurcpy.commit()

            # create views and triggers in postgresql copy
            cols = ""
            newcols = ""
            hcols = ['ogc_fid'] + sum([[brch+'_rev_begin', brch+'_rev_end',
                                        brch+'_parent', brch+'_child'] for brch in pg_branches(pcur, schema)], [])
            for res in pcurcpy.execute(self.__pragmaTableInfo(wcs, table)).fetchall():
                if res[1].lower() not in [c.lower() for c in hcols]:
                    cols += quote_ident(res[1]) + ", "
                    newcols += "new."+quote_ident(res[1])+", "
            cols = cols[:-2]
            newcols = newcols[:-2]  # remove last coma

            pcurcpy.execute("CREATE VIEW "+wcs+"."+table+"_view "+"AS "
                            "SELECT row_number() over() AS ROWID, ogc_fid, "+cols+" "
                            "FROM "+wcs+"."+table+" WHERE "+branch+"_rev_end IS NULL "
                            "AND "+branch+"_rev_begin IS NOT NULL")

            max_fid_sub = ("( SELECT MAX(max_fid) FROM ( SELECT MAX(ogc_fid) AS "
                           "max_fid FROM {wcs}.{table} UNION SELECT max_pk AS max_fid "
                           "FROM {wcs}.initial_revision WHERE table_name = '{wcs}.{table}') alias )".format(wcs=wcs, table=table))
            current_rev_sub = ("(SELECT rev FROM {wcs}.initial_revision "
                               "WHERE table_name = '{table}')".format(wcs=wcs, table=table))

            # when we edit something old, we insert and update parent
            constraint_before = constraint_builder.get_referencing_constraint('update', table)
            constraint_after = constraint_builder.get_referenced_constraint('update', table)
            sql = f"""
                
            CREATE OR REPLACE FUNCTION 
            {wcs}.update_old_{table}() RETURNS trigger AS $$\n
            DECLARE\n
            cnt integer;\n
           BEGIN\n
            RAISE NOTICE 'update_old.1 new.ogc_fid=%', new.ogc_fid;
            
            SELECT COUNT(*) FROM {wcs}.{table} 
            WHERE ogc_fid = new.ogc_fid AND ({branch}_rev_begin <= 
            {current_rev_sub} ) into cnt;\n
            RAISE NOTICE 'update_old.2';
            IF cnt > 0 THEN\n
            
            {constraint_before}
            RAISE NOTICE 'update_old.3';
            INSERT INTO {wcs}.{table} 
            (ogc_fid, {cols}, {branch}_rev_begin, 
            {branch}_parent)
            VALUES 
            ({max_fid_sub}+1, {newcols}, {current_rev_sub}+1, old.ogc_fid);\n
            RAISE NOTICE 'update_old.4';
            
            UPDATE {wcs}.{table} SET {branch}_rev_end = {current_rev_sub}, 
            {branch}_child = {max_fid_sub} WHERE ogc_fid = old.ogc_fid;\n
            RAISE NOTICE 'update_old.5';
            {constraint_after}
            END IF;\n
            RAISE NOTICE 'update_old.6';
            RETURN OLD;\n
            END;\n$$ LANGUAGE plpgsql;"""
            if DEBUG:
                print(sql)
            pcurcpy.execute(sql)
            pcurcpy.execute("DROP TRIGGER IF EXISTS "
                            "update_old_"+table+"_view ON "+wcs+"."+table+"_view ")

            sql = """CREATE TRIGGER 
                update_old_{table}_conflicts 
                INSTEAD OF UPDATE ON {wcs}.{table}_view 
                FOR EACH ROW 
                EXECUTE PROCEDURE {wcs}.update_old_{table}()\n""".format(
                wcs=wcs,
                table=table)
            if DEBUG:
                print(sql)
            pcurcpy.execute(sql)
            pcurcpy.commit()

            # when we edit something new, we just update
            pcurcpy.execute(f"""
            CREATE OR REPLACE FUNCTION
            {wcs}.update_new_{table}() RETURNS trigger AS $$
            DECLARE
            cnt integer;
            BEGIN
            RAISE NOTICE 'update_new.1 new.ogc_fid=%', new.ogc_fid;
            SELECT COUNT(*) FROM {wcs}.{table}
            WHERE ogc_fid = new.ogc_fid AND ({branch}_rev_begin >
            {current_rev_sub}) into cnt;
            RAISE NOTICE 'update_new.2';
            IF cnt > 0 THEN
            {constraint_before}
            RAISE NOTICE 'update_new.3';
                            
            INSERT INTO {wcs}.{table} (ogc_fid, {cols}, {branch}_rev_begin, {branch}_parent)
            VALUES (new.ogc_fid, {newcols}, {current_rev_sub}+1, (SELECT
            {branch}_parent FROM {wcs}.{table}
            WHERE ogc_fid = new.ogc_fid))
            ON CONFLICT (ogc_fid)
            DO
            UPDATE
            SET (ogc_fid, {cols}, {branch}_rev_begin, {branch}_parent)
            =
            (new.ogc_fid, {newcols}, {current_rev_sub}+1, (SELECT
            {branch}_parent FROM {wcs}.{table}
            WHERE ogc_fid = new.ogc_fid));
                            
            RAISE NOTICE 'update_new.4';
            {constraint_after}
            end if;
            RAISE NOTICE 'update_new.5';
            RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;""")
            
            pcurcpy.execute("DROP TRIGGER IF EXISTS "
                            "update_new_"+table+"_view ON "+wcs+"."+table+"_view ")
            pcurcpy.execute("CREATE TRIGGER "
                            "update_new_"+table+"_conflicts "
                            "INSTEAD OF UPDATE ON "+wcs+"."+table+"_view "
                            "FOR EACH ROW "
                            "EXECUTE PROCEDURE "+wcs+".update_new_"+table+"();\n")
            pcurcpy.commit()

            constraint = constraint_builder.get_referencing_constraint('insert', table)
            pcurcpy.execute("CREATE OR REPLACE FUNCTION " +
                            wcs+".insert_"+table+"() RETURNS trigger AS $$\n"
                            "DECLARE\n"
                            "BEGIN\n" +
                            constraint + "\n" +
                            "INSERT INTO "+wcs+"."+table+" " +
                            "(ogc_fid, "+cols+", "+branch+"_rev_begin) "
                            "VALUES "
                            "("+max_fid_sub+"+1, "+newcols +
                            ", "+current_rev_sub+"+1);\n" +
                            "RETURN NEW;\n"
                            "END;\n"
                            "$$ LANGUAGE plpgsql;")

            pcurcpy.execute("DROP TRIGGER IF EXISTS "
                            "insert_"+table+" ON "+wcs+"."+table)
            pcurcpy.execute("CREATE TRIGGER "
                            "insert_new_"+table+"_conflicts "
                            "INSTEAD OF INSERT ON "+wcs+"."+table + "_view "
                            " FOR EACH ROW "
                            "EXECUTE PROCEDURE "+wcs+".insert_"+table+"();")

            pcurcpy.commit()

            constraint = constraint_builder.get_referenced_constraint('delete', table)
            pcurcpy.execute("CREATE OR REPLACE FUNCTION " +
                            wcs+".delete_"+table+"() RETURNS trigger AS $$\n"
                            "BEGIN\n" +
                            constraint + "\n" +
                            # update it if its old
                            "UPDATE "+wcs+"."+table+" "
                            "SET "+branch+"_rev_end = "+current_rev_sub+" "
                            "WHERE ogc_fid = old.ogc_fid "
                            "AND "+branch+"_rev_begin < "+current_rev_sub+"+1;\n"
                            # update its parent if its modified
                            "UPDATE "+wcs+"."+table+" "
                            "SET "+branch+"_rev_end = "+current_rev_sub+", "+branch+"_child = NULL "
                            "WHERE "+branch+"_child = old.ogc_fid;\n"
                            # delete it if its new and remove it from child
                            "UPDATE "+wcs+"."+table+" "
                            "SET "+branch+"_child = NULL "
                            "WHERE "+branch+"_child = old.ogc_fid "
                            "AND "+branch+"_rev_begin = "+current_rev_sub+"+1;\n"
                            "DELETE FROM "+wcs+"."+table+" "
                            "WHERE ogc_fid = old.ogc_fid "
                            "AND "+branch+"_rev_begin = "+current_rev_sub+"+1;\n"
                            "RETURN NEW;\n"
                            "END;\n"
                            "$$ LANGUAGE plpgsql;")
            pcurcpy.execute("DROP TRIGGER IF EXISTS "
                            "delete_"+table+" ON "+wcs+"."+table)
            pcurcpy.execute("CREATE TRIGGER "
                            "delete_new_"+table+"_conflicts "
                            "INSTEAD OF DELETE ON "+wcs+"."+table + "_view "
                            "FOR EACH ROW "
                            "EXECUTE PROCEDURE "+wcs+".delete_"+table+"();")
            pcurcpy.commit()

        # Remove temp views after sqlite file is written
        for i in temp_view_names:
            del_view_str = "DROP VIEW IF EXISTS " + i
            pcur.execute(del_view_str)
            pcur.commit()

        pcurcpy.execute("""CREATE TABLE %s.wcs_con as SELECT '%s'::text as connection""" % (wcs,
                                                                                            pg_conn_info.replace("'", "''")))
        pcurcpy.commit()
        pcur.close()
        pcurcpy.close()

    def unresolved_conflicts(self, connection):
        (pg_conn_info, wcs, pg_conn_info_copy) = connection
        """return a list of tables with unresolved conflicts"""
        found = []
        pcurcpy = Db(psycopg2.connect(pg_conn_info_copy))
        pcurcpy.execute("SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='"+wcs+"' AND table_name LIKE '%_conflicts'")
        for table_conflicts in pcurcpy.fetchall():
            if DEBUG:
                print('table_conflicts:', table_conflicts[0])
            pcurcpy.execute("SELECT * FROM "+table_conflicts[0])
            if pcurcpy.fetchone():
                found.append(table_conflicts[0][:-10])
        pcurcpy.commit()
        pcurcpy.close()
        return found

    def commit(self, connection, commit_msg, commit_user=''):
        """merge modifications into database
        returns the number of updated layers"""
        # get the target revision from the postgresql copy
        # create the diff in postgres
        # load the diff in pg copy
        # detect conflicts
        # merge changes and update target_revision
        # delete diff
        (pg_conn_info, wcs, pg_conn_info_copy) = connection
        unresolved = self.unresolved_conflicts(
            [pg_conn_info, wcs, pg_conn_info_copy])
        if unresolved:
            raise RuntimeError("There are unresolved conflicts in "
                               + wcs+" for table(s) "+', '.join(unresolved))

        late_by = self.late([pg_conn_info, wcs, pg_conn_info_copy])
        if late_by:
            raise RuntimeError("Working copy "+wcs +
                               " is not up to date. "
                               "It's late by "+str(late_by)+" commit(s).\n\n"
                               "Please update before commiting your modifications")

        pcurcpy = Db(psycopg2.connect(pg_conn_info_copy))
        pcurcpy.execute("SELECT rev, branch, table_schema, table_name "
                        "FROM "+wcs+".initial_revision")
        versioned_layers = pcurcpy.fetchall()

        if not versioned_layers:
            raise RuntimeError("Cannot find a versioned layer in "+wcs)

        check_unique_constraints(Db(psycopg2.connect(pg_conn_info)),
                                 pcurcpy, wcs)
        
        schema_list = {}  # for final cleanup
        nb_of_updated_layer = 0
        next_rev = 0
        for [rev, branch, table_schema, table] in versioned_layers:
            diff_schema = (table_schema+"_"+branch+"_"+str(rev) +
                           "_to_"+str(rev+1)+"_diff")

            if next_rev:
                assert(next_rev == rev + 1)
            else:
                next_rev = rev + 1

            pcurcpy.execute("DROP TABLE IF EXISTS "+wcs+"."+table+"_diff CASCADE")

            sql = """CREATE TABLE {wcs}.{table}_diff as SELECT * FROM {wcs}.{table} WHERE 1=2""".format(
                wcs=wcs, table=table)
            if DEBUG:
                print(sql)
            pcurcpy.execute(sql)
            pcurcpy.commit()

            pcurcpy.execute("INSERT INTO "+wcs+"."+table+"_diff "
                            "SELECT * "
                            "FROM "+wcs+"."+table+" "
                            "WHERE "+branch+"_rev_end = "+str(rev)+" "
                            "OR "+branch+"_rev_begin > "+str(rev))
            pcurcpy.execute("SELECT ogc_fid FROM "+wcs+"."+table+"_diff")
            there_is_something_to_commit = pcurcpy.fetchone()
            if DEBUG:
                print("there_is_something_to_commit ",
                      there_is_something_to_commit)
            pcurcpy.commit()

            # Better if we could have a QgsDataSourceURI.username()
            try:
                pg_username = pg_conn_info.split(
                    ' ')[3].replace("'", "").split('=')[1]
            except (IndexError):
                pg_username = ''

            pcur = Db(psycopg2.connect(pg_conn_info))
            pkey = pg_pk(pcur, table_schema, table)
            pgeom = pg_geom(pcur, table_schema, table)

            # import layers in postgis schema
            pcur.execute("SELECT schema_name FROM information_schema.schemata "
                         "WHERE schema_name = '"+diff_schema+"'")
            if not pcur.fetchone():
                schema_list[diff_schema] = pg_conn_info
                pcur.execute("CREATE SCHEMA "+diff_schema)
            pcur.execute("DROP TABLE IF EXISTS "+diff_schema+"."+table+"_diff")
            pcur.commit()
            cmd = ['ogr2ogr',
                   '-preserve_fid',
                   '-f',
                   'PostgreSQL',
                   'PG:"'+pg_conn_info+'"',
                   '-lco', 'FID='+pkey,
                   '-lco', 'GEOMETRY_NAME={}'.format(pgeom),
                   'PG:"'+pg_conn_info_copy+'"',
                   wcs+"."+table+"_diff",
                   '-nln', diff_schema+'.'+table+"_diff"]

            if DEBUG:
                print(' '.join(cmd))
            os.system(' '.join(cmd))

            for l in pcur.execute("select * from geometry_columns").fetchall():
                if DEBUG:
                    print(l)

            # remove dif table and geometry column
            pcurcpy.execute("DELETE FROM geometry_columns "
                            "WHERE f_table_name = '"+wcs+"."+table+"_diff'")
            pcurcpy.execute("DROP TABLE "+wcs+"."+table+"_diff")

            if not there_is_something_to_commit:
                if DEBUG:
                    print("nothing to commit for ", wcs+"."+table)
                pcur.close()
                continue

            nb_of_updated_layer += 1

            pcur.execute("SELECT rev FROM "+table_schema+".revisions "
                         "WHERE rev = "+str(rev+1))
            if not pcur.fetchone():
                if DEBUG:
                    print("inserting rev ", str(rev+1))
                pcur.execute("INSERT INTO "+table_schema+".revisions "
                             "(rev, commit_msg, branch, author) "
                             "VALUES ("+str(rev+1)+", '" +
                             escape_quote(commit_msg)+"', '"+branch+"',"
                             "'"+os_info()+":"+get_username()+"."+pg_username+"."+commit_user+"')")

            other_branches = pg_branches(pcur, table_schema).remove(branch)
            other_branches = other_branches if other_branches else []
            other_branches_columns = sum([
                [brch+'_rev_begin', brch+'_rev_end',
                 brch+'_parent', brch+'_child']
                for brch in other_branches], [])
                
            pkey = pg_pk( pcur, table_schema, table )
            pcur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{table_schema}'
                AND table_name = '{table}'
                """.format(table_schema=table_schema, table=table))
            history_columns = [pkey]
            cols = ""
            coli = ""
            allcols = pcur.fetchall()
            for col, dtype in allcols:
                if col not in history_columns:
                    # Workaround for uuid type
                    if dtype == 'uuid':
                        cols = 'uuid('+quote_ident(col)+')'+", "+cols
                    else:
                        cols = quote_ident(col)+", "+cols
                    coli = quote_ident(col)+", "+coli
                else:
                    coli = quote_ident(col)+", "+coli
                    cols = "(SELECT max({pkey}) FROM {table_schema}.{table}) + row_number() over() as ".format(table_schema=table_schema,
                                                           table=table, pkey=pkey)+quote_ident(pkey)+", "+cols
                    
            cols = cols[:-2] # remove last coma and space
            coli = coli[:-2]
            # insert inserted and modified
            sql = """INSERT INTO {table_schema}.{table} ({coli}) 
                SELECT {cols} FROM {diff_schema}.{table}_diff 
                WHERE {branch}_rev_begin = {rev}""".format(table_schema=table_schema,
                                                           table=table, coli=coli, cols=cols, diff_schema=diff_schema, branch=branch, rev=str(rev+1))
            if DEBUG:
                print(sql)
            pcur.execute(sql)

            # update deleted and modified
            pcur.execute("UPDATE "+table_schema+"."+table+" AS dest "
                         "SET ("+branch+"_rev_end, "+branch+"_child)"
                         "=(src."+branch+"_rev_end, src."+branch+"_child) "
                         "FROM "+diff_schema+"."+table+"_diff AS src "
                         "WHERE dest."+pkey+" = src."+pkey+" "
                         "AND src."+branch+"_rev_end = "+str(rev))

            pcur.commit()
            pcur.close()

            pcurcpy.commit()

        if nb_of_updated_layer:
            for [rev, branch, table_schema, table] in versioned_layers:
                pcur = Db(psycopg2.connect(pg_conn_info))
                pkey = pg_pk(pcur, table_schema, table)
                pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions")
                [rev] = pcur.fetchone()
                pcur.execute("SELECT MAX("+pkey+") FROM " +
                             table_schema+"."+table)
                [max_pk] = pcur.fetchone()
                if not max_pk:
                    max_pk = 0
                pcur.close()
                pcurcpy.execute("UPDATE "+wcs+".initial_revision "
                                "SET rev = "+str(rev) +
                                ", max_pk = "+str(max_pk)+" "
                                "WHERE table_schema = '"+table_schema+"' "
                                "AND table_name = '"+table+"' "
                                "AND branch = '"+branch+"'")

        pcurcpy.commit()
        pcurcpy.close()

        # cleanup diffs in postgis
        for schema, conn_info in schema_list.items():
            pcur = Db(psycopg2.connect(conn_info))
            pcur.execute("DROP SCHEMA "+schema+" CASCADE")
            pcur.commit()
            pcur.close()

        return nb_of_updated_layer
