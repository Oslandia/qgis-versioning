
"""
/***************************************************************************
 versioning
                                 A QGIS plugin
 postgis database versioning
                              -------------------
        begin                : 2018-06-14
        copyright            : (C) 2018 by Oslandia
        email                : infos@oslandia.com
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

from collections import OrderedDict


def patch_trigger(cur, schema, method, table, sql, before):

    if method == "update" and cur.isSpatialite():
        patch_trigger_from_name(cur, schema, f"{method}_old_{table}", sql, before)
        patch_trigger_from_name(cur, schema, f"{method}_new_{table}", sql, before)
    else:
        patch_trigger_from_name(cur, schema, f"{method}_{table}", sql, before)


def patch_trigger_from_name(cur, schema, trigger_name, sql, before):

    if cur.isPostgres():

        cur.execute(f"""
        SELECT pr.prosrc FROM pg_proc pr, pg_namespace ns
        WHERE pr.pronamespace = ns.oid AND nspname = '{schema}'
        AND pr.proname = '{trigger_name}';
        """)

        trigger_sql = cur.fetchone()[0]
        if before:
            trigger_sql = trigger_sql.replace("BEGIN", "BEGIN\n" + sql)
        else:
            trigger_sql = trigger_sql.replace("RETURN", sql + "\nRETURN")

        cur.execute(f"""
        CREATE OR REPLACE FUNCTION {schema}.{trigger_name}()
        RETURNS trigger AS
        $$
        {trigger_sql}
        $$ LANGUAGE plpgsql
        """)

    else:
        cur.execute(f"""
        SELECT sql FROM sqlite_master WHERE type = 'trigger'
        AND name = '{trigger_name}'
        """)

        trigger_sql = cur.fetchone()[0]
        if before:
            trigger_sql = trigger_sql.replace("BEGIN", "BEGIN\n" + sql)
        else:
            trigger_sql = trigger_sql.replace("END", sql + "\nEND")

        cur.execute(f"DROP TRIGGER {trigger_name}")
        cur.execute(trigger_sql)


def setup_constraint_triggers(b_cur, wc_cur, b_schema, wc_schema, tables):
    """

    Build and setup unique and foreign key constraints on table views

    :param b_cur: base cursor (must be opened and valid)
    :param wc_cur: working copy cursor (must be opened and valid)
    :param b_schema: base schema
    :param wc_schema: working copy schema
    :param tables: list of tables

    """
    # Get unique and foreign key constraints
    b_cur.execute(f"""
    SELECT table_from, columns_from, defaults_from, table_to,
    columns_to, updtype, deltype
    FROM {b_schema}.versioning_constraints
    """)

    tables_wo_schema = [table[1] for table in tables]

    # Build trigger upon this contraints and setup on view
    for idx, (table_from, columns_from, defaults_from, table_to, columns_to,
              updtype, deltype) in enumerate(b_cur.fetchall()):

        # table is not being checkout
        if table_from not in tables_wo_schema:
            continue

        # build fully qualified table
        q_table_from = ((wc_schema + "." + table_from) if wc_schema
                        else table_from)
        q_table_to = ((wc_schema + "." + table_to) if wc_schema and table_to
                      else table_to)

        # build table name for trigger name
        name_table_from = q_table_from.replace('.', '_')
        name_table_to = q_table_from.replace('.', '_')

        # unique constraint
        if not table_to:

            for method in ['insert', 'update']:

                # check if unique keys already exist
                when_filter = "(SELECT COUNT(*) FROM {}_view WHERE {}) != 0".format(
                    q_table_from,
                    " AND ".join(["{0} = NEW.{0}".format(column) for column in columns_from]))

                # check if unique keys have been modified
                if method == 'update': 
                    when_filter += " AND " + " AND ".join(["NEW.{0} != OLD.{0}".format(column)
                                                         for column in columns_from]) 

                keys = ",".join(columns_from)

                # postgres requests
                if wc_cur.isPostgres():

                    sql = f"""
                    IF {when_filter} THEN
                    RAISE EXCEPTION 'Fail {q_table_from} {keys} unique constraint';
                    END IF;
                    """
                    patch_trigger(wc_cur, wc_schema, method, table_from, sql, True)

                # spatialite requests
                else:

                    sql = f'SELECT RAISE(FAIL, "Fail {q_table_from} {keys} unique constraint") WHERE {when_filter};'
                    patch_trigger(wc_cur, wc_schema, method, table_from, sql, True)

        # foreign key constraint
        else:

            assert(len(columns_from) == len(columns_to))

            # check if referenced keys exists
            when_filter = "(SELECT COUNT(*) FROM {}_view WHERE {}) = 0".format(
                q_table_to, " AND ".join(
                    [f"(NEW.{column_from} IS NULL "
                     f"OR {column_to} = NEW.{column_from})"
                     for column_to, column_from
                     in zip(columns_to, columns_from)]))

            keys = ",".join(columns_from)

            for method in ['insert', 'update']:

                # postgres requests
                if wc_cur.db_type == 'pg : ':

                    sql = f"""IF {when_filter} THEN
                    RAISE EXCEPTION 'Fail {keys} foreign key constraint';
                    END IF;"""

                    patch_trigger(wc_cur, wc_schema, method,
                                  table_from, sql, True)

                # spatialite requests
                else:

                    sql = f'SELECT RAISE(FAIL, "Fail {keys} foreign key constraint") WHERE {when_filter};'
                    patch_trigger(wc_cur, wc_schema,
                                  method, table_from, sql, True)

            # special actions when a referenced key is updated/deleted
            for method in ['delete', 'update']:

                # check if referencing keys have been modified
                where = None
                if method == 'update':
                    where = "({})".format(
                        " OR ".join(["NEW.{0} != OLD.{0}".format(column)
                                     for column in columns_to]))
                else:
                    where = "True"

                action_type = updtype if method == 'update' else deltype

                # cascade
                action = ""
                if action_type == 'c':
                    for column_from, column_to in zip(columns_from, columns_to):
                        col_where = where + f" AND {column_from} = OLD.{column_to}"
                        if method == 'update':
                            action += f"UPDATE {q_table_from}_view SET {column_from} = NEW.{column_to} WHERE {col_where};"""
                        else:
                            action += f"DELETE FROM {q_table_from}_view WHERE {col_where};"

                # set null or set default
                elif action_type == 'n' or action_type == 'd':
                    for column_from, column_to, default_from in zip(columns_from, columns_to, defaults_from):
                        new_value = "NULL" if action_type == 'n' or default_from is None else default_from
                        col_where = where + f" AND {column_from} = OLD.{column_to}"
                        action += f"UPDATE {q_table_from}_view SET {column_from} = {new_value} WHERE {col_where};"""

                # fail
                else:
                    keys_label = ",".join(columns_to) + (" is" if len(columns_to) == 1 else " are")
                    action += (f"""IF {where} THEN RAISE EXCEPTION '{keys_label} still referenced by {q_table_from}'; END IF;"""
                               if wc_cur.db_type == 'pg : '
                               else f"""SELECT RAISE(FAIL, "{keys_label} still referenced by {q_table_from}") WHERE {where};""")

                # postgres requests
                if wc_cur.db_type == 'pg : ':

                    patch_trigger(wc_cur, wc_schema, method,
                                  table_to, action, False)

                # spatialite requests
                else:

                    sql = f'SELECT RAISE(FAIL, "Fail {q_table_from} {keys} unique constraint") WHERE {when_filter};'
                    patch_trigger(wc_cur, wc_schema, method, table_to, action, False)

    wc_cur.commit()
