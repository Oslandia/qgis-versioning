
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

import platform
import re
import psycopg2
from sqlite3 import dbapi2
import getpass
import sys
import traceback
import codecs
import os
from itertools import zip_longest
from collections import defaultdict

# Deactivate stdout (like output of print statements) because windows
# causes occasional "IOError [Errno 9] File descriptor error"
# Not needed when there is a way to run QGIS in console mode in Windows.
iswin = any(platform.win32_ver())
if iswin:
    sys.stdout = open(os.devnull, 'w')


class Db(object):
    """Basic wrapper arround DB cursor that allows for logging SQL commands"""
    def __init__(self, con, filename=''):
        """The passed connection must be closed with close()"""
        self.con = con
        if isinstance(con, dbapi2.Connection):
            self.db_type = 'sp : '
            self.con.enable_load_extension(True)
            self.con.execute("SELECT load_extension('mod_spatialite')")
        else:
            self.db_type = 'pg : '
        self.cur = self.con.cursor()
        if filename:
            self.log = codecs.open(filename, 'w', 'utf-8')
            self.log.write('-- opening connection\n')
        else:
            self.log = None
        self.begun = False
        self._verbose = False

    def isPostgres(self):
        """Returns True this cursor is a Postgres cursor"""
        return self.db_type == 'pg : '

    def isSpatialite(self):
        """Returns True this cursor is a Spatialite cursor"""
        return self.db_type == 'sp : '

    def hasrow(self):
        """Test if previous execute returned rows"""
        if self._verbose:
            print(self.db_type, self.cur.rowcount, ' rows returned')
        return self.cur.rowcount > 0

    def verbose(self, verbose):
        """Set verbose level"""
        self._verbose = verbose

    def execute(self, sql):
        """Execute SQL command"""
        if not self.begun:
            self.begun = True
            if self._verbose:
                print(self.db_type, 'BEGIN;')
            if self.log:
                self.log.write('BEGIN;\n')
        if self._verbose:
            print(self.db_type, sql, ';')
        if self.log:
            self.log.write(sql+';\n')
        try:
            self.cur.execute(sql)
            return self.cur
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
            print(self.db_type, 'END;')
        if self.log:
            self.log.write('END;\n')
        self.begun = False
        self.con.commit()

    def close(self):
        """Close DB connection"""
        if self.begun:
            if self._verbose:
                print(self.db_type, 'END;')
            if self.log:
                self.log.write('END;\n')
        if self.log:
            self.log.write('-- closing connection\n')
        self.con.close()


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


def pg_pk(cur, schema_name, table_name):
    """Fetch the primary key of the specified postgis table"""
    cur.execute("SELECT quote_ident(a.attname) as column_name "
                "FROM pg_index i "
                "JOIN pg_attribute a ON a.attrelid = i.indrelid "
                "AND a.attnum = ANY(i.indkey) "
                "WHERE i.indrelid = '\""+schema_name+'"."'+table_name
                + "\"'::regclass "
                "AND i.indisprimary")
    if not cur.hasrow():
        raise RuntimeError("table "+schema_name + "." + table_name +
                           " does not have a primary key")
    [pkey] = cur.fetchone()
    return pkey


def pg_geoms(cur, schema_name, table_name):
    """Fetch the list of geometry columns of the specified postgis table,
    empty if none"""
    cur.execute("SELECT f_geometry_column FROM geometry_columns "
                "WHERE f_table_schema = '"+schema_name+"' "
                "AND f_table_name = '"+table_name+"'")
    return [geo[0] for geo in cur.fetchall()]


def pg_geom(cur, schema_name, table_name):
    """Fetch the first geometry column of the specified postgis table,
    empty string if none"""
    geoms = pg_geoms(cur, schema_name, table_name)
    if not geoms:
        return ''
    elif len(geoms) == 1:
        return geoms[0]
    elif 'VERSIONING_GEOMETRY_COLUMN' in os.environ:
        if os.environ['VERSIONING_GEOMETRY_COLUMN'] in geoms:
            return os.environ['VERSIONING_GEOMETRY_COLUMN']
        else:
            raise RuntimeError('more than one geometry column in '
                               + schema_name + '.' + table_name
                               + ' but none is '
                               + os.environ['VERSIONING_GEOMETRY_COLUMN'] +
                               ' (i.e. the value of '
                               + 'VERSIONING_GEOMETRY_COLUMN) ')
    elif 'geometry' in geoms:
        return 'geometry'
    else:
        raise RuntimeError(
            'more than one geometry column in '
            + schema_name + '.' + table_name +
            ' but the environment variable VERSIONING_GEOMETRY_COLUMN '
            'is not defined and the geometry column name is not geometry')


def pg_branches(pcur, schema):
    """returns a list of branches for this schema"""
    pcur.execute("SELECT DISTINCT branch FROM "+schema+".revisions")
    return [res for [res] in pcur.fetchall()]


def pg_array_elem_type(cur, schema, table, column):
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
                           + schema + '.' + table + ' is not an ARRAY')
    [res] = cur.fetchone()
    return res


def get_pg_users_list(pg_conn_info):
    pcur = Db(psycopg2.connect(pg_conn_info))
    pcur.execute("select usename from pg_user order by usename ASC")
    pg_users_list = pcur.fetchall()
    pg_users_str_list = []
    for i in pg_users_list:
        pg_users_str_list.append(str(i[0]))
    pcur.close()
    return pg_users_str_list


def get_actual_pk(uri, pg_conn_info):
    """Get actual PK from corresponding table or view.  The result serves to
    ascertain that the PK found by QGIS for PG views matches the real PK.
    """
    mtch = re.match(r'(.+)_([^_]+)_rev_(head|\d+)', uri.schema())
    pcur = Db(psycopg2.connect(pg_conn_info))
    actual_pk = pg_pk(pcur, mtch.group(1), uri.table())
    pcur.close()
    return actual_pk


def preserve_fid(pkid, fetchall_tuple):
    # This is a hack because os.system does not scale in MS Windows.
    # We need to create a view, then emulate the "preserve_fid" behaviour of
    # ogr2ogr.  A select * in the new view will generate random ogc_fid values
    # which means we cannot commit modifications after a checkout.
    # pkid = name of pkid as a string
    # fetchall_tuple = a list of column names returned as tuples by fetchall()

    str_list = []
    for i in fetchall_tuple:
        str_list.append(str(i[0]))

    replaceText = pkid
    replaceData = pkid + ' as ogc_fid'
    pos = str_list.index(replaceText)
    str_list[pos] = replaceData
    columns_str = ', '.join(str_list)
    return columns_str


def escape_quote(msg):
    """quote single quotes"""
    return str.replace(str(msg), "'", "''")


def quote_ident(ident):
    """Add quotes around identifier if it contains spaces"""
    if ident.find(' '):
        return '"'+ident+'"'
    else:
        return ident


def get_username():
    """Returns user name"""
    return getpass.getuser()


def get_pkeys(b_cur, schema, table):
    """Returns real primary keys for given table (the one defined
    before we historize the table
    """

    b_cur.execute(f"""
    SELECT UNNEST(columns_from) FROM epanet.versioning_constraints
    WHERE table_from = '{table}' and table_to is NULL;""")

    return [res[0] for res in b_cur.fetchall()]


def add_connected_features(pcur, tables, mode):
    """ Add referenced table in tables according to given mode

    mode can be referenced or referencing whether you want to checkout
    the foreign key referenced feature or whether you want to checkout
    the features referencing your primary key
    """

    assert(mode == "referenced" or mode == "referencing")

    table_orig = "table_from" if mode == "referenced" else "table_to"
    table_ref = "table_to" if mode == "referenced" else "table_from"

    columns_orig = "columns_from" if mode == "referenced" else "columns_to"
    columns_ref = "columns_to" if mode == "referenced" else "columns_from"

    tables_cpy = tables.copy()

    # We checkout all referenced features
    for (schema, table, branch), feature_list in tables_cpy.items():

        pcur.execute(f"""
        SELECT {table_ref}, {columns_orig}, {columns_ref}
        FROM {schema}.versioning_constraints
        WHERE {table_orig} = '{table}'
        AND {table_ref} IS NOT NULL;
        """)

        # get referenced feature ids for each referenced table
        for t_ref, cols_orig, cols_ref in pcur.fetchall():

            key = (schema, t_ref, branch)

            # t_ref is already in tables and not filtered (feature_list is
            # None) so we have all feature
            if (key in tables_cpy and tables_cpy[key] is None):
                continue

            pkey_orig = pg_pk(pcur, schema, table)
            pkey_ref = pg_pk(pcur, schema, t_ref)

            where_filter = " AND ".join(
                ["torig.{} = tref.{}".format(col_orig, col_ref)
                 for col_orig, col_ref in
                 zip(cols_orig, cols_ref)])

            if feature_list:
                fids = ",".join([str(fid) for fid in feature_list])
                where_filter += f" AND torig.{pkey_orig} in ({fids})"

            pcur.execute(f"""
            SELECT tref.{pkey_ref}
            FROM {schema}.{t_ref} tref, {schema}.{table} torig
            WHERE {where_filter}
            """)

            tables[key] |= set(
                [res[0] for res in pcur.fetchall()])


def get_checkout_tables(connection, table_names, selected_feature_lists):
    """ Build and return tables to be checkout according to given
    pg_tables parameter.

    :param connection: database connection string
    :param table_names: table name list
    :param selected_feature_lists: selected feature list (ids)

    """
    pcur = Db(psycopg2.connect(connection))

    # We build table dictionnary with associated feature set
    tables = defaultdict(set)
    for table_name, feature_list in zip_longest(
            table_names, selected_feature_lists):
        schema, table = table_name.split('.')
        if not (schema and table and schema[-9:] == "_rev_head"):
            raise RuntimeError("Schema names must end with "
                               "suffix _branch_rev_head")

        schema, _, branch = schema[:-9].rpartition('_')

        # None means that we retrieve all feature
        tables[(schema, table, branch)] = (set(feature_list) if feature_list
                                           else None)

    add_connected_features(pcur, tables, "referenced")
    add_connected_features(pcur, tables, "referencing")

    # transform set in list before return
    return {table: list(fids) if fids is not None else []
            for table, fids in tables.items()}
