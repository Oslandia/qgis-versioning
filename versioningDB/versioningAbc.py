# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .postgresqlLocal import pgVersioning
from .spatialite import spVersioning

TYPE = ('postgres', 'spatialite')

class versioningAbc(object):
    
    def __init__(self, connection, typebase):
        assert(isinstance(connection, list) and 
               len(connection) == 2 and
               typebase in TYPE)
        
        self.typebase = typebase
        # spatialite : [sqlite_filename, pg_conn_info]
        # postgres : [pg_conn_info, working_copy_schema]
        self.connection = connection
        if self.typebase == 'spatialite':
            self.ver = spVersioning()
        else:
            self.ver = pgVersioning()
            
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
        