# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .postgresqlServer import pgVersioningServer
from .spatialite import spVersioning
from .postgresqlLocal import pgVersioningLocal

TYPE = ('postgres', 'spatialite', 'pgDistant')
CONNECTIONS = {'postgres': 2, 'spatialite': 2, 'pgDistant': 3}

class versioningAbc(object):
    
    def __init__(self, connection, typebase):
        assert(isinstance(connection, list) and 
               typebase in TYPE and
               len(connection) == CONNECTIONS[typebase])
        
        self.typebase = typebase
        # spatialite : [sqlite_filename, pg_conn_info]
        # postgres : [pg_conn_info, working_copy_schema]
        # pgDistant : [pg_conn_info, working_copy_schema, pg_conn_info_out]
        self.connection = connection
        if self.typebase == 'spatialite':
            self.ver = spVersioning()
        elif self.typebase == 'pgDistant':
            self.ver = pgVersioningLocal()
        else:
            self.ver = pgVersioningServer()
            
    def revision(self):
        return self.ver.revision(self.connection)
    
    def late(self ):
        return self.ver.late(self.connection)
    
    def update(self):
        self.ver.update(self.connection)
    
    def checkout(self, pg_table_names, selected_feature_lists = []):
        self.ver.checkout(self.connection, pg_table_names, selected_feature_lists)
    
    def unresolved_conflicts(self):
        return self.ver.unresolved_conflicts(self.connection)
    
    def commit(self, commit_msg, commit_user = ''):
        return self.ver.commit(self.connection, commit_msg, commit_user)
        