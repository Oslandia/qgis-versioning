
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

        # When we trigger on update on delete, We can't use the view
        # because it will check the foreign key constraint before we made our
        # modifications. So we want to update the table behind the view. so
        # we get table_from name in base schema (except when wc_schema is None,
        # which mean we are in spatialite)
        b_table_from = ((b_schema + "." + table_from) if wc_schema
                        else table_from)

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
                if wc_cur.db_type == 'pg : ':

                    wc_cur.execute(f"""
                    CREATE FUNCTION {method}_check{idx}_unique_{name_table_from}()
                    RETURNS trigger AS
                    $$
                    BEGIN
                    IF {when_filter} THEN
                    RAISE EXCEPTION 'Fail {q_table_from} {keys} unique constraint';
                    END IF;
                    RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                    """)

                    wc_cur.execute(f"""
                    CREATE TRIGGER {method}_check{idx}_unique_{name_table_from}_trigger
                    INSTEAD OF {method}
                    ON {q_table_from}_view
                    FOR EACH ROW
                    EXECUTE PROCEDURE {method}_check{idx}_unique_{name_table_from}();
                    """)

                # spatialite requests
                else:

                    wc_cur.execute(f"""
                    CREATE TRIGGER {method}_check{idx}_unique_{name_table_from}
                    INSTEAD OF {method} ON {q_table_from}_view
                    FOR EACH ROW
                    WHEN {when_filter}
                    BEGIN
                    SELECT RAISE(FAIL, "Fail {q_table_from} {keys} unique constraint");
                    END;""")

        # foreign key constraint
        else:

            assert(len(columns_from) == len(columns_to))

            # check if referenced keys exists
            when_filter = "(SELECT COUNT(*) FROM {}_view WHERE {}) = 0".format(
                q_table_to,
                " AND ".join(["{} = NEW.{}".format(column_to, column_from)
                              for column_to, column_from in zip(columns_to, columns_from)]))

            keys = ",".join(columns_from)

            for method in ['insert','update']:

                # postgres requests
                if wc_cur.db_type == 'pg : ':

                    wc_cur.execute(f"""
                    CREATE FUNCTION {method}_check{idx}_fkey_{name_table_from}_to_{name_table_to}()
                    RETURNS trigger AS
                    $$
                    BEGIN
                    IF {when_filter} THEN
                    RAISE EXCEPTION 'Fail {keys} foreign key constraint';
                    END IF;
                    RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                    """)

                    wc_cur.execute(f"""
                    CREATE TRIGGER {method}_check{idx}_fkey_{name_table_from}_to_{name_table_to}_trigger
                    INSTEAD OF {method}
                    ON {q_table_from}_view
                    FOR EACH ROW
                    EXECUTE PROCEDURE {method}_check{idx}_fkey_{name_table_from}_to_{name_table_to}();
                    """)

                # spatialite requests
                else:

                    wc_cur.execute(f"""
                    CREATE TRIGGER {method}_check{idx}_fkey_{name_table_from}_to_{name_table_to}
                    INSTEAD OF {method} ON {q_table_from}_view
                    FOR EACH ROW
                    WHEN {when_filter}
                    BEGIN
                    SELECT RAISE(FAIL, "Fail {keys} foreign key constraint");
                    END;""")

            # special actions when a referenced key is updated/deleted
            for method in ['delete', 'update']:

                # check if referencing keys have been modified
                when_filter = ""
                if method == 'update':
                    when_filter += " OR ".join(["NEW.{0} != OLD.{0}".format(column)
                                                for column in columns_to])

                action_type = updtype if method == 'update' else deltype

                # cascade
                action = ""
                if action_type == 'c':
                    for column_from, column_to in zip(columns_from, columns_to):
                        where = f"WHERE {column_from} = OLD.{column_to}"
                        if method == 'update':
                            action += f"UPDATE {b_table_from} SET {column_from} = NEW.{column_to} {where};"""
                        else:
                            action += f"DELETE FROM {q_table_from}_view {where};"

                # set null or set default
                elif action_type == 'n' or action_type == 'd':
                    for column_from, column_to, default_from in zip(columns_from, columns_to, defaults_from):
                        new_value = "NULL" if action_type == 'n' or default_from is None else default_from
                        where = f"WHERE {column_from} = OLD.{column_to}"
                        action += f"UPDATE {b_table_from} SET {column_from} = {new_value} {where};"""

                # fail
                else:
                    keys_label = ",".join(columns_to) + (" is" if len(columns_to) == 1 else " are")
                    action += (f"""RAISE EXCEPTION '{keys_label} still referenced by {q_table_from}';"""
                               if wc_cur.db_type == 'pg : '
                               else f"""SELECT RAISE(FAIL, "{keys_label} still referenced by {q_table_from}");""")

                # postgres requests
                if wc_cur.db_type == 'pg : ':

                    body = (f"IF {when_filter} THEN {action} END IF;"
                            if when_filter else action)

                    to_return = "OLD" if method == 'delete' else "NEW"

                    wc_cur.execute(f"""
                    CREATE FUNCTION {method}_check{idx}_fkey_modifed_{name_table_from}_to_{name_table_to}()
                    RETURNS trigger AS
                    $$
                    BEGIN
                    {body}
                    RETURN {to_return};
                    END;
                    $$ LANGUAGE plpgsql
                    """)

                    wc_cur.execute(f"""
                    CREATE TRIGGER {method}_check{idx}_fkey_modifed_{name_table_from}_to_{name_table_to}_trigger
                    INSTEAD OF {method}
                    ON {q_table_to}_view
                    FOR EACH ROW
                    EXECUTE PROCEDURE {method}_check{idx}_fkey_modifed_{name_table_from}_to_{name_table_to}();
                    """)

                # spatialite requests
                else:

                    when_filter = "WHEN " + when_filter if when_filter else ""
                    wc_cur.execute(f"""
                    CREATE TRIGGER {method}_check{idx}_fkey_modifed_{name_table_from}_to_{name_table_to}
                    INSTEAD OF {method} ON {q_table_to}_view
                    FOR EACH ROW
                    {when_filter}
                    BEGIN
                    {action}
                    END;""")

    wc_cur.commit()
