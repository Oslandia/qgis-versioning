
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

from .utils import get_pkeys


class Constraint:

    def __init__(self, table_from, columns_from, defaults_from, table_to,
                 columns_to, updtype, deltype):
        """ Construct a unique or foreign key constraint

        :param table_from: referencing table
        :param columns_from: referencing columns
        :param defaults_from: default values
        :param table_to: referenced table (None if constraint is unique key)
        :param columns_to: referenced columns (empty if constraint is
        unique key)
        :param updtype: update type (cascade, set default, set null, restrict)
        :param deltype: delete type (cascade, set default, set null, restrict)

        """
        assert(not columns_to or len(columns_from) == len(columns_to))

        self.table_from = table_from
        self.columns_from = columns_from
        self.defaults_from = defaults_from
        self.table_to = table_to
        self.columns_to = columns_to
        self.updtype = updtype
        self.deltype = deltype

    def get_q_table_from(self, schema):
        """ Build and return fully qualified table from

        :param schema: table schema
        :returns: fully qualified table name
        :rtype: str

        """
        return ((schema + "." + self.table_from) if schema
                else self.table_from)

    def get_q_table_to(self, schema):
        """ Build and return fully qualified table to

        :param schema: table schema
        :returns: fully qualified table name
        :rtype: str

        """

        return ((schema + "." + self.table_to) if schema
                else self.table_to)


class ConstraintBuilder:

    def __init__(self, b_cur, wc_cur, b_schema, wc_schema):
        """ Constructor to build unique and foreign key constraint

        :param b_cur: base cursor (must be opened and valid)
        :param wc_cur: working copy cursor (must be opened and valid)
        :param b_schema: base schema
        :param wc_schema: working copy schema

        """
        self.b_cur = b_cur
        self.wc_cur = wc_cur
        self.b_schema = b_schema
        self.wc_schema = wc_schema

        b_cur.execute(f"""
        SELECT table_from, columns_from, defaults_from, table_to,
        columns_to, updtype, deltype
        FROM {b_schema}.versioning_constraints
        """)

        # build two dict to speed access to constraint from
        # referencing and referenced table
        self.referencing_constraints = {}
        self.referenced_constraints = {}

        # Build trigger upon this contraints and setup on view
        for (table_from, columns_from, defaults_from, table_to, columns_to,
             updtype, deltype) in b_cur.fetchall():

            constraint = Constraint(table_from, columns_from, defaults_from,
                                    table_to, columns_to, updtype, deltype)

            self.referencing_constraints.setdefault(table_from, []).append(
                constraint)

            if table_to:
                self.referenced_constraints.setdefault(table_to, []).append(
                    constraint)

    def get_referencing_constraint(self, method, table):
        """ Build and return unique and foreign key referencing constraints
        sql for given table

        :param method: insert, update or delete
        :param table: the referencing table for which we need to build
        constraints

        """

        sql_constraint = ""
        if (table not in self.referencing_constraints
                or method not in ['insert', 'update']):
            return sql_constraint

        for constraint in self.referencing_constraints.get(table, []):

            # unique constraint
            if not constraint.table_to:

                q_table_from = constraint.get_q_table_from(self.wc_schema)

                # check if unique keys already exist
                when_filter = "(SELECT COUNT(*) FROM {}_view WHERE {}) != 0".format(
                    q_table_from,
                    " AND ".join(["{0} = NEW.{0}".format(column) for column in constraint.columns_from]))

                # check if unique keys have been modified
                if method == 'update': 
                    when_filter += " AND " + " AND ".join(["NEW.{0} != OLD.{0}".format(column)
                                                           for column in constraint.columns_from]) 

                keys = ",".join(constraint.columns_from)

                # postgres requests
                if self.wc_cur.isPostgres():

                    sql_constraint += f"""IF {when_filter} THEN
                    RAISE EXCEPTION 'Fail {q_table_from} {keys} unique constraint';
                    END IF;
                    """

                # spatialite requests
                else:

                    sql_constraint += f'SELECT RAISE(FAIL, "Fail {q_table_from} {keys} unique constraint") WHERE {when_filter};'

            # foreign key constraint
            else:

                q_table_to = constraint.get_q_table_to(self.wc_schema)

                # check if referenced keys exists
                when_filter = "(SELECT COUNT(*) FROM {}_view WHERE {}) = 0".format(
                    q_table_to, " AND ".join(
                        [f"(NEW.{column_from} IS NULL "
                         f"OR {column_to} = NEW.{column_from})"
                         for column_to, column_from
                         in zip(constraint.columns_to, constraint.columns_from)]))

                keys = ",".join(constraint.columns_from)

                # postgres requests
                if self.wc_cur.db_type == 'pg : ':

                    sql_constraint += f"""IF {when_filter} THEN
                    RAISE EXCEPTION 'Fail {keys} foreign key constraint';
                    END IF;"""

                # spatialite requests
                else:

                    sql_constraint += f'SELECT RAISE(FAIL, "Fail {keys} foreign key constraint") WHERE {when_filter};'

        return sql_constraint

    def get_referenced_constraint(self, method, table):
        """ Build and return foreign key referenced constraints sql for given table

        :param method: insert, update or delete
        :param table: the referenced table for which we need to build
        constraints

        """

        sql_constraint = ""
        if table not in self.referenced_constraints or method not in ['delete', 'update']:
            return sql_constraint

        for constraint in self.referenced_constraints.get(table, []):

            # check if referenced keys have been modified
            where = None
            if method == 'update':
                where = "({})".format(
                    " OR ".join(["NEW.{0} != OLD.{0}".format(column)
                                 for column in constraint.columns_to]))
            else:
                where = "True"

            action_type = (constraint.updtype if method == 'update'
                           else constraint.deltype)

            q_table_from = constraint.get_q_table_from(self.wc_schema)

            # cascade
            if action_type == 'c':
                for column_from, column_to in zip(constraint.columns_from, constraint.columns_to):
                    col_where = where + f" AND {column_from} = OLD.{column_to}"
                    if method == 'update':
                        sql_constraint += f"UPDATE {q_table_from}_view SET {column_from} = NEW.{column_to} WHERE {col_where};"""
                    else:
                        sql_constraint += f"DELETE FROM {q_table_from}_view WHERE {col_where};"

            # set null or set default
            elif action_type == 'n' or action_type == 'd':
                for column_from, column_to, default_from in zip(constraint.columns_from, constraint.columns_to, constraint.defaults_from):
                    new_value = "NULL" if action_type == 'n' or default_from is None else default_from
                    col_where = where + f" AND {column_from} = OLD.{column_to}"
                    sql_constraint += f"UPDATE {q_table_from}_view SET {column_from} = {new_value} WHERE {col_where};"""

            # fail
            else:

                where += " AND (SELECT COUNT(*) FROM {}_view WHERE {}) > 0".format(
                    q_table_from,
                    " AND ".join(f"{column_from} = OLD.{column_to}" for column_from, column_to in
                                 zip(constraint.columns_from, constraint.columns_to)))
                
                keys_label = ",".join(constraint.columns_to) + (" is" if len(constraint.columns_to) == 1 else " are")
                sql_constraint += (f"""IF {where} THEN RAISE EXCEPTION '{keys_label} still referenced by {q_table_from}'; END IF;"""
                                   if self.wc_cur.db_type == 'pg : '
                                   else f"""SELECT RAISE(FAIL, "{keys_label} still referenced by {q_table_from}") WHERE {where};""")

        return sql_constraint


def check_unique_constraints(b_cur, wc_cur, wc_schema):

    wc_cur.execute("SELECT rev, branch, table_schema, table_name "
                   f"FROM {wc_schema}.initial_revision")
    versioned_layers = wc_cur.fetchall()

    errors = []
    for [rev, branch, b_schema, table] in versioned_layers:

        # Spatialite
        if wc_cur.isSpatialite():
            table_w_revs = f"{table}"
            vid = "ogc_fid"

        # PgServer
        elif b_cur is wc_cur:
            table_w_revs = f"{wc_schema}.{table}_diff"
            vid = "versioning_id"

        # PgLocal
        else:
            table_w_revs = f"{wc_schema}.{table}"
            vid = "ogc_fid"

        pkeys = get_pkeys(b_cur, b_schema, table)
        pkey_list = ",".join(["trev." + pkey for pkey in pkeys])

        wc_cur.execute(f"""
        -- INSERTED PKEY
        SELECT {pkey_list}
        FROM {table_w_revs} trev
        WHERE {branch}_rev_end is NULL
        AND {branch}_parent is NULL
        AND {branch}_rev_begin > {rev}
        UNION
        -- UPDATED PKEY
        SELECT {pkey_list}
        FROM {table_w_revs} trev, {table_w_revs} trev2
        WHERE trev.{branch}_parent IS NOT NULL
        AND trev.{branch}_rev_begin > 1
        AND trev.{branch}_parent = trev2.{vid}
        AND trev.id != trev2.id;
        """)

        new_keys = wc_cur.fetchall()

        if not new_keys:
            continue

        # search in database if there are some working copy new key that
        # already exist.
        new_key_list = ",".join(["({})".format(
            ",".join([str(int_key)for int_key in new_key]))
                                 for new_key in new_keys])

        b_cur.execute(f"""
        SELECT {pkey_list}
        FROM {b_schema}.{table} trev
        WHERE {branch}_rev_end is NULL
        AND {branch}_parent is NULL
        INTERSECT
        SELECT *
        FROM (VALUES {new_key_list}) AS new_keys""")

        def to_string(rec):
            return " and ".join(
                [f"{pkey}={value}" for pkey, value in zip(pkeys, rec)])

        errors += ["   {}.{} : {}".format(wc_schema, table, to_string(res))
                   for res in b_cur.fetchall()]

    if errors:
        raise RuntimeError("Some new or updated row violate the primary key"
                           " constraint in base database :\n{}".format(
                               "\n".join(errors)))
